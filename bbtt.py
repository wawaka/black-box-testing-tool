#!/usr/bin/env python3

import sys
sys.path.append('.')

assert sys.version_info >= (3, 9), "you must use Python >= 3.9"

import argparse
import codecs
import collections.abc
import importlib
import json
import copy
import re
import struct
import time
from pprint import pprint as pp

import easybase
import yaml
from dateutil.parser import parse
from kafka import KafkaConsumer, KafkaProducer


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


def format_recursive(fmt, substitutions):
    if type(fmt) is dict:
        return {format_recursive(k, substitutions): format_recursive(v, substitutions) for k, v in fmt.items()}
    if type(fmt) is list:
        return [format_recursive(e, substitutions) for e in fmt]
    if type(fmt) is str:
        return fmt.format(**substitutions)
    return fmt


class KafkaSendAction:
    def __init__(self, **kwargs):
        self.producer = KafkaProducer(bootstrap_servers=kwargs['brokers'])
        self.topic = kwargs['topic']
        self.template = (yaml.full_load(open(kwargs['template_file'])) if 'template_file' in kwargs else {}) | kwargs.get('template', {})
        self.format = yaml.full_load(open(kwargs['format_file'])) if 'format_file' in kwargs else None

    def serialize_object(self, obj):
        if self.format:
            obj = format_recursive(self.format, obj)
        return to_json(obj).encode('utf8')

    def exec(self, kwargs):
        for msg in kwargs['messages']:
            obj = self.template | msg
            value = self.serialize_object(obj)
            self.producer.send(self.topic, value=value)
            # print(value)
        print(f"\t⏩\tsent {len(kwargs['messages'])} messages")


class KafkaCheckAction:
    def __init__(self, **kwargs):
        self.consumer = KafkaConsumer(kwargs['topic'], bootstrap_servers=kwargs['brokers'])
        self.consumer.poll(1000) # without this line consumer does not actually subscribes for topic and does not start track messages
        self.template = (yaml.full_load(open(kwargs['template_file'])) if 'template_file' in kwargs else {}) | kwargs.get('template', {})
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


class PPrintAction:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def exec(self, kwargs):
        pp(kwargs)


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
        elif v is None:
            self.v = v
        else:
            raise Exception(f"unexpected value {v}")

    def encode(self):
        return self.v

    def __eq__(self, arg):
        assert type(self) is type(arg)
        return self.v == arg.v

    def __repr__(self):
        if self.v is None:
            return 'missing'
        return repr(self.v)


class HBaseAction:
    def __init__(self, **kwargs):
        self.connection = easybase.Connection(kwargs['host'])
        self.htable = self.connection.table(kwargs['table'])

    def exec(self, kwargs):
        for cmd in kwargs['commands']:
            if cmd['type'] == 'put':
                for rowkey, vs in cmd['rows'].items():
                    print(f"\t>>>> executing command {cmd['type']!r} for row {rowkey!r}")
                    if vs is None:
                        self.htable.delete(rowkey)
                        print(f"\t🗑\trow {rowkey!r} deleted")
                    else:
                        to_put = {}
                        to_delete = []
                        for k, v in vs.items():
                            if v is None:
                                to_delete.append(k)
                            else:
                                to_put[k] = HBaseValueProxy(v)

                        if to_delete:
                            self.htable.delete(rowkey, to_delete)
                            print(f"\t🗑\trow {rowkey!r} deleted: {to_delete!r}")

                        if to_put:
                            self.htable.put(rowkey, to_put)
                            print(f"\t💾\trow {rowkey!r} put: {to_put!r}")

            elif cmd['type'] == 'check':
                for rowkey, vs in cmd['rows'].items():
                    print(f"\t>>>> executing command {cmd['type']!r} for row {rowkey!r}")
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
                                    print(f"\t✅\trow {rowkey!r}: key {k!r} has value {actual_value!r} (expected {expected_value!r})")
                                else:
                                    print(f"\t❌\trow {rowkey!r}: key {k!r} has value {actual_value!r} (expected {expected_value!r})")
                            else:
                                if v is None:
                                    print(f"\t✅\trow {rowkey!r}: key {k!r} is missing (expected {expected_value!r})")
                                else:
                                    print(f"\t❌\trow {rowkey!r}: key {k!r} is missing (expected {expected_value!r})")

            elif cmd['type'] == 'delete':
                for rowkey in cmd['rows']:
                    print(f"\t>>>> executing command {cmd['type']!r} for row {rowkey!r}")
                    self.htable.delete(rowkey)
                    print(f"\t🗑\trow {rowkey!r} deleted")

            else:
                raise Exception(f"unexpected command type {cmd['type']}")

ACTIONS = {
    'sleep': SleepAction,
    'print': PrintAction,
    'pprint': PPrintAction,
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

def run_test(test, actions):
    for i_action, test_action in enumerate(test['actions'], 1):
        test_action_name = test_action['action']
        try:
            action_runner = actions[test_action_name]
        except KeyError:
            raise Exception(f"action {test_action_name!r} has not been defined")

        print(f"executing action {i_action}:{test_action_name}")
        action_runner.exec(test_action)

def run_test_config(config, functions, args):
    if 'name' in config:
        print(f"running test config {config['name']!r}")

    actions = init_actions(config['actions'])

    for i, test in enumerate(config['tests'], 1):
        test['loop'] = test.get('loop', 1)
        while test['loop'] > 0:
            if not(args.test_number is None or args.test_number == i):
                break

            print(f"#{i} running test {test.get('name')!r}")

            run_test(evaluate_functions(test, functions), actions)

            test['loop'] -= 1

class Function:
    @staticmethod
    def init(name, value):
        if type(value) is str:
            lines = value.splitlines()
            if len(lines) > 1:
                return ExecFunction(name, value)
        return EvalFunction(name, value)


class EvalFunction(Function):
    def __init__(self, name, value):
        self.code = value
        self.imports = {}

    def __call__(self, value):
        f_globals = self.imports
        f_locals = {'arg': value}
        try:
            rc = eval(self.code, f_globals, f_locals)
        except NameError as e:
            missing_name = re.findall("name '(\w+)' is not defined", str(e))[0]
            self.imports[missing_name] = importlib.import_module(missing_name)
            return self(value)
        except Exception as e:
            print(f"Exception while evaluating code {self.code!r}: {e}")
            # print(f"{f_globals=}")
            # print(f"{f_locals=}")
            raise

        return rc


class ExecFunction:
    @staticmethod
    def fake_import(_code):
        exec(_code)
        del _code
        return locals()

    def __init__(self, name, value):
        self.function = self.fake_import(value)[name]

    def __call__(self, value):
        if type(value) is dict:
            return self.function(**value)
        if type(value) is list:
            return self.function(*value)
        return self.function(value)


def init_functions(config):
    functions = {}
    for f_name, f_value in config.items():
        functions[f_name] = Function.init(f_name, f_value)
    return functions


def setattr_recursive(obj, d):
    for k, v in d.items():
        attr = getattr(obj, k)
        if type(v) is dict:
            setattr_recursive(attr, v)
        elif type(v) is list:
            attr_type = type(attr)
            dir_attr_type = dir(attr_type)
            if 'add' in dir_attr_type:
                for e in v:
                    setattr_recursive(attr.add(), e)
            elif 'append' in dir_attr_type:
                for e in v:
                    attr.append(e)
            else:
                raise Exception(f"no method to set array data: {obj}={d}")
        else:
            setattr(obj, k, v)


def function_protobuf(pb_module_name, pb_class_name, pb_fields):
    import importlib
    pb_module = importlib.import_module(pb_module_name)

    pb_class = getattr(pb_module, pb_class_name)
    pb_obj = pb_class()
    setattr_recursive(pb_obj, pb_fields)

    return pb_obj.SerializeToString()


def evaluate_functions(value, functions):
    if type(value) is list:
        return [evaluate_functions(v, functions) for v in value]

    if type(value) is dict:
        for k in value:
            if k == f'{FUNCTION_PREFIX}const':
                return CONSTANTS[value[k]]

        value = {k: evaluate_functions(v, functions) for k, v in value.items()}

        for k in value:
            if k == f'{FUNCTION_PREFIX}protobuf':
                return function_protobuf(**value[k])

            if k.startswith(FUNCTION_PREFIX):
                f_name = k[len(FUNCTION_PREFIX):]
                function = functions[f_name]
                try:
                    return function(value[k])
                except Exception as e:
                    print(f"Exception while calling {value}: {e}")
                    raise

    return value


def load_config(path):
    return yaml.full_load(open(path))


def load_configs(paths):
    final_config = {
        'functions': {},
        'constants': {},
        'actions': {
            'sleep': {
                'type': 'sleep',
                'time': 1,
            },
            'print': {
                'type': 'print',
            },
            'pprint': {
                'type': 'pprint',
            },
        },
    }
    for path in paths:
        config = load_config(path)
        final_config = update(final_config, config)
    return final_config


def main(args):
    config = load_configs(args.config_files)
    # pp(config)

    functions = init_functions(config['functions'])
    global CONSTANTS
    CONSTANTS = evaluate_functions(config['constants'], functions)
    # pp(CONSTANTS)

    # config = evaluate_functions(config, functions)
    # pp(config)

    run_test_config(config, functions, args)


if __name__ == "__main__":
    main(parse_args())
