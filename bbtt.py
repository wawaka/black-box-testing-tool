#!/usr/bin/env python3

import re
import sys
import json
import time
import struct
import codecs
import argparse
import importlib
import collections.abc

from pprint import pprint as pp

from dateutil.parser import parse
from kafka import KafkaConsumer, KafkaProducer
import easybase
import yaml


FUNCTION_PREFIX = '$'


def update(d, u):
    for k, v in u.items():
        if isinstance(v, collections.abc.Mapping):
            d[k] = update(d.get(k, {}), v)
        else:
            d[k] = v
    return d

def parse_args():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        '-n', '--test-number',
        type = int,
    )
    parser.add_argument(
        'config_files',
        nargs = '+'
    )
    args = parser.parse_args()
    return args


def dictdiff(d1, d2):
    diff = {}
    for k2, v2 in d2.items():
        if k2 in d1:
            if d1[k2] != v2:
                diff[k2] = v2
        else:
            diff[k2] = v2
    return diff


def poll_until_empty(consumer, timeout_ms):
    values = []
    while True:
        data = consumer.poll(timeout_ms)
        if not data:
            return values
        for tp, msgs in data.items():
            for msg in msgs:
                values.append(json.loads(msg.value))


def to_json(d):
    return json.dumps(d, sort_keys=True)


class KafkaSendAction:
    def __init__(self, **kwargs):
        self.producer = KafkaProducer(bootstrap_servers=kwargs['brokers'])
        self.topic = kwargs['topic']
        self.template = (json.load(open(kwargs['template_file'])) if 'template_file' in kwargs else {}) | kwargs.get('template', {})
        self.format = json.load(open(kwargs['format_file'])) if 'format_file' in kwargs else None

    def serialize_object(self, obj):
        if self.format:
            obj = self.format_object(self.format, obj)
        return to_json(obj).encode('utf8')

    @classmethod
    def format_string(cls, fmt, obj):
        return fmt.format(**obj)

    @classmethod
    def format_object(cls, fmt, obj):
        out = {}
        for fk, fv in fmt.items():
            k = cls.format_string(fk, obj) if type(fk) is str else fk
            v = cls.format_string(fv, obj) if type(fv) is str else fv
            out[k] = v
        return out

    def exec(self, kwargs):
        for msg in kwargs['messages']:
            obj = self.template | msg
            value = self.serialize_object(obj)
            self.producer.send(self.topic, value=value)
        print(f"\t⏩\tsent {len(kwargs['messages'])} messages")


class KafkaCheckAction:
    def __init__(self, **kwargs):
        self.consumer = KafkaConsumer(kwargs['topic'], bootstrap_servers=kwargs['brokers'])
        self.consumer.poll(1000) # without this line consumer does not actually subscribes for topic and does not start track messages
        self.template = (json.load(open(kwargs['template_file'])) if 'template_file' in kwargs else {}) | kwargs.get('template', {})
        self.consume_timeout = kwargs.get('consume_timeout', 1)

    def exec(self, kwargs):
        values = poll_until_empty(self.consumer, int(self.consume_timeout * 1000))
        received = sorted(values, key=to_json)
        expected = sorted([self.template | msg for msg in kwargs['messages']], key=to_json)
        if expected == received:
            print(f"\t✅\treceived {len(values)} messages as expected")
        else:
            print(f"\t❌\tmessages received are different from what was expected")

            print(f"RECEIVED {len(received)} messages:")
            for msg in received:
                msg = dictdiff(self.template, msg)
                print(f"\t❗\t{to_json(msg)}")

            print(f"EXPECTED {len(expected)} messages:")
            for msg in expected:
                msg = dictdiff(self.template, msg)
                print(f"\t❗\t{to_json(msg)}")


class PrintAction:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def exec(self, kwargs):
        print(f"\t{kwargs!r}")


class SleepAction:
    def __init__(self, **kwargs):
        self.time = kwargs['time']

    def exec(self, kwargs):
        time_to_sleep = kwargs.get('time', self.time)
        print(f"\t⏳\twaiting for {time_to_sleep} seconds...")
        time.sleep(time_to_sleep)


class HBaseValueProxy:
    def __init__(self, v):
        if isinstance(v, str):
            self.v = v.encode()
        elif isinstance(v, bytes):
            self.v = v
        else:
            raise Exception(f"unexpected value {v}")

    def encode(self):
        return self.v

    def __eq__(self, arg):
        assert type(self) is type(arg)
        return self.v == arg.v

    def __repr__(self):
        return repr(self.v)

def convert_vs(vs):
    return {k: HBaseValueProxy(v) for k, v in vs.items()}

class HBaseAction:
    def __init__(self, **kwargs):
        self.connection = easybase.Connection(kwargs['host'])
        self.htable = self.connection.table(kwargs['table'])

    def exec(self, kwargs):
        for cmd in kwargs['commands']:
            if cmd['type'] == 'put':
                for rowkey, vs in cmd['rows'].items():
                    value = convert_vs(vs)
                    self.htable.put(rowkey, value)
                    print(f"\t💾\tput {rowkey!r} {value!r}")
            elif cmd['type'] == 'delete':
                for rowkey in cmd['rows']:
                    self.htable.delete(rowkey)
                    print(f"\t🗑\tdeleted {rowkey!r}")
            elif cmd['type'] == 'check':
                for rowkey, vs in cmd['rows'].items():
                    if vs is None:
                        row = self.htable.row(rowkey)
                        if not row:
                            print(f"\t✅\trow {rowkey!r}: missing as expected")
                        else:
                            print(f"\t❌\trow {rowkey!r}: has value {row!r} (expected missing)")
                    else:
                        row = self.htable.row(rowkey, columns=list(vs.keys()))
                        for k, v in vs.items():
                            expected_value = HBaseValueProxy(v)
                            if k in row:
                                actual_value = HBaseValueProxy(row[k])
                                if actual_value == expected_value:
                                    pass
                                    print(f"\t✅\trow {rowkey!r}: key {k!r} has value {actual_value!r} (expected {expected_value!r})")
                                else:
                                    print(f"\t❌\trow {rowkey!r}: key {k!r} has value {actual_value!r} (expected {expected_value!r})")
                            else:
                                print(f"\t❌\trow {rowkey!r}: key {k!r} is missing (expected {expected_value!r})")


ACTIONS = {
    'sleep': SleepAction,
    'print': PrintAction,
    'hbase': HBaseAction,
    'kafka_send': KafkaSendAction,
    'kafka_check': KafkaCheckAction,
}


def init_actions(actions_config):
    actions = {}

    for action_name, action_params in actions_config.items():
        action_type = action_params['type']
        try:
            action_class = ACTIONS[action_type]
        except KeyError:
            raise Exception(f"unexpected action type {action_type}")
        print(f"initializing action {action_name}...")
        actions[action_name] = action_class(**action_params)

    return actions


def run_test_config(config, args):
    print(f"running test config {config['name']!r}")

    actions = init_actions(config['actions'])

    for i, test in enumerate(config['tests'], 1):
        if not(args.test_number is None or args.test_number == i):
            pass
            # print(f"#{i} skipping test {test['name']!r}")
        else:
            print(f"#{i} running test {test['name']!r}")

            for i_action, test_action in enumerate(test['actions'], 1):
                test_action_name = test_action['action']
                try:
                    action_runner = actions[test_action_name]
                except KeyError:
                    raise Exception(f"action {test_action_name!r} has not been defined")

                print(f"executing action {i_action}:{test_action_name}")
                action_runner.exec(test_action)


class Function:
    def __init__(self, value):
        kwargs = value if type(value) is dict else {'code': value}
        self.code = kwargs['code']
        self.imports = {name: importlib.import_module(name) for name in kwargs.get('imports', [])}

    def __call__(self, args, kwargs):
        env = {'arg': args[0], 'args': args} | self.imports
        f_globals = env
        f_locals = env
        try:
            rc = eval(self.code, f_globals, f_locals)
        except NameError as e:
            missing_name = re.findall("name '(\w+)' is not defined", str(e))[0]
            self.imports[missing_name] = importlib.import_module(missing_name)
            return self(args, kwargs)

        return rc


def init_functions(config):
    functions = {}
    for f_name, f_value in config.items():
        functions[f_name] = Function(f_value)
    return functions


def evaluate_functions(functions, value):
    if type(value) is list:
        return [evaluate_functions(functions, v) for v in value]

    if type(value) is dict:
        for k in value:
            if k.startswith(FUNCTION_PREFIX):
                f_name = k[len(FUNCTION_PREFIX):]
                function = functions[f_name]
                args = value[k]
                if type(args) is not list:
                    args = [args]
                return function(args, value)

        return {k: evaluate_functions(functions, v) for k, v in value.items()}

    return value


def load_config(path):
    return yaml.full_load(open(path))


def load_configs(paths):
    final_config = {}
    for path in paths:
        config = load_config(path)
        final_config = update(final_config, config)
    return final_config


def main(args):
    config = load_configs(args.config_files)
    # pp(config)

    functions = init_functions(config.get('functions', {}))
    config = evaluate_functions(functions, config)
    # pp(config)
    run_test_config(config, args)


if __name__ == "__main__":
    main(parse_args())
