This is a tool for blackbox testing any system using inputs and outputs (kafka and hbase are currently supported)

# Running
To run a test you have to call the tool and pass paths of all configuration files as command line arguments:
```shell
./bbtt.py config1.yaml config2.yaml
```
If multiple files are specified, it's contents is merged key-by key, overwriting values from left to right.

# Configuration file
To make a test you have to write a configuration file in yaml format.
Configuration file can have many sections but only 2 are required:
1. actions - defines types of actions
2. tests - defines sequence of tests to run that use pre-defined actions

## Actions
There are several action types supported:
1. `kafka_send` - sending json-based messages to any kafka topic(s)
2. `kafka_check` - reading and checking json-based messages from any kafka topic(s)
3. `hbase` - execute various command on a hbase database
4. `sleep` - delays execution for specified amount of time
5. `print` - prints some values for debug purposes
6. `pprint` - pretty prints some values for debug purpose


# Features

## Kafka support
The tool supports 2 types of kafka actions:
1. `kafka_send` - sending json-based messages to any kafka topic(s)
2. `kafka_check` - reading and checking json-based messages from any kafka topic(s)

Example:
```yaml
actions:
    send_orders:
        type: kafka_send
        brokers: localhost:9092
        topic: orders
    check_orders:
        type: kafka_check
        brokers: localhost:9092
        topic: orders
tests:
-   actions:
    -   action: send_orders
        messages: # send some messages to a kafka topic
        -   {"userid": 100, "orderid": 1}
        -   {"userid": 200, "orderid": 2}
    -   action: check_orders
        messages: # read messages from the same topic and ensure they are as expected (order does not matter)
        -   {"userid": 100, "orderid": 1}
        -   {"userid": 200, "orderid": 2}
```

More kafka examples are available in file [examples/kafka.yaml](examples/kafka.yaml)

## Kafka message template
Sometimes it's not convenient to specify full message for every test as they can be quite huge but only differ in some fields.
For this there is template feature. It allows to specify message template for this kafka action. All messages used with this action will override template fields before sending/checking. You have to define template in the `template` key of corresponding kafka action.
Example:
```yaml
actions:
    send_orders:
        type: kafka_send
        brokers: localhost:9092
        topic: orders
        template: {address: 1 Wall Street, phone: 1234567890, userid: 0}
    check_orders:
        type: kafka_check
        brokers: localhost:9092
        topic: orders
        template: {address: 1 Wall Street, phone: 1234567890, userid: 0}
tests:
-   actions:
    -   action: send_orders
        messages: # send some messages to a kafka topic
        -   {userid: 100, orderid: 1} # the actual message sent would be {userid: 100, orderid: 1, address: 1 Wall Street, phone: 1234567890}
    -   action: check_orders
        messages: # read messages from the same topic and ensure they are as expected (order does not matter)
        -   {userid: 100, orderid: 1} # the actual message checked would be {userid: 100, orderid: 1, address: 1 Wall Street, phone: 1234567890}
```

## Kafka message template file
If embedding full template message into the configuration file is not an option, you can refer to an external template in a file by using
option `template_file` of a kafka action.
Example:
```yaml
actions:
    send_orders:
        type: kafka_send
        brokers: localhost:9092
        topic: orders
        template_file: order_template.json
```

## Kafka message format file
...

## Hbase support
The tool supports modifications and checking of data in hbase.
In order for it to work you have to enable thrift2 api in your hbase: `hbase thrift2`
There are 3 commands supported for hbase action type:
1. `put` - set values for rows and fields
2. `check` - compare values of rows and fields to expected values
3. `delete` - delete rows
```yaml
actions:
    users_db:
        type: hbase
        host: localhost
        table: XXX:users
tests:
-   actions:
    -   action: user_db
        commands:
        -   type: put
            rows:
                user0: null # put null for a row means delete row
                user1: {f:name: John Brown, f:role: admin}
                user2: {f:name: Donald Trump, f:role: null} # put null for a field means delete field
        -   type: check
            rows:
                user0: null # check null for a row means check if does not exist
                user1: {f:name: John Brown, f:role: admin}
                user2: {f:name: Donald Trump, f:role: null} # check null for a field means check that field does not exist
        -   type: delete
            rows: # delete just deletes rows, same as put with null
            - user0
            - user1
            - user2
```

More hbase examples are available in file [examples/hbase.yaml](examples/hbase.yaml)

## Functions
Functions allow to define and evaluate arbitrary python code with parameters.
You have to define them in `functions` section of your configuration file:
```yaml
functions:
    function_name: python code
```
To evaluate a function you have to use code `{$function_name: value}`
This will replace `{$function_name: value}` with the result of calling function `function_name` with parameter `value`
Inside the body of a function a parameter is available by the fixed name `arg`
Example:
```yaml
functions:
    square: arg * arg
    cube: arg * arg * arg
actions:
    print:
        type: pprint
tests:
-   actions:
    -   action: print
        test_square: {$square: 9} # will be replaced with 81
        test_cube: {$cube: 5} # will be replaced with 125
```
Some useful functions are available in [examples/functions.yaml](examples/functions.yaml)

## Constants
Sometimes you want to define test-wide contsants, for example, ids.
For that, you can use constants. They are defined in `constants` section of a configuration file.
To use them, you have to use pre-defined function `$const` with constant name as its argument.
Values for constants can also be results of function evaluation.
Example:
```yaml
functions:
    now: int(time.time())
constants:
    userid: 100500
    timestamp: {$now: 0} # argument is ignored
actions:
    print:
        type: pprint
tests:
-   actions:
    -   action: print
        test_userid: {$const: userid} # will be replaced with 100500
```

## Protobuf
To generate protobuf-serialized messages you can use function `$protobuf`.
It requires to pass an object with 3 fields:
1. `pb_module_name` for name protobuf python module
2. `pb_class_name` for name of class
3. `pb_fields` for dict of fields for this class
Example:
```yaml
$protobuf: 
    pb_module_name: addressbook_pb2
    pb_class_name: AddressBook
    pb_fields: {...}
```

Protobuf example config is available in [examples/protobuf.yaml](examples/protobuf.yaml)
