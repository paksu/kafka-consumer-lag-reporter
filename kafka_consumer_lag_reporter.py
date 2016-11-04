import subprocess
import argparse
import datetime
from influxdb import InfluxDBClient


# VERSION 0.1
# from https://github.com/vainiot/kafka-consumer-lag-reporter

OUTPUT_KEYS = ['group', 'topic', 'partition', 'current_offset', 'log_end_offset', 'lag']


def parse_output(input_from_checker):
    """
    Parses the output from kafka-consumer-groups.sh, converts metrics in to integers and returns
    a list of dicts from each row as a response
    """
    output = []
    for line in input_from_checker:
        # Skip header
        if 'GROUP, TOPIC, PARTITION' not in line:
            columns = line.split(', ')

            # Only pick the columns we are interested in
            group = columns[0]
            topic = columns[1]
            metric_columns = [int(c) for c in columns[2:6]]

            key_and_value_pairs = zip(OUTPUT_KEYS, [group, topic] + metric_columns)
            output.append(dict(key_and_value_pairs))

    return output


def to_influxdb_json(parsed_output):
    json_body = [
        {
            "measurement": "kafka.consumer_offset",
            "tags": {
                "topic": line['topic'],
                "group": line['group'],
                "partition": line['partition']
            },
            "time": str(datetime.datetime.now()),
            "fields": {
                "current_offset": line['current_offset'],
                "log_end_offset": line['log_end_offset'],
                "lag": line['lag']
            }
        } for line in parsed_output
    ]
    return json_body


def get_kafka(args):
    """
    Gets consumer offsets from kafka via kafka-consumer-groups.sh

    Should work on Kafka 0.8.0.2 and 0.9.0.1
    """
    # append trailing slash
    if args.kafka_dir[-1] != "/":
        args.kafka_dir = args.kafka_dir + "/"

    params = [
        '{}bin/kafka-consumer-groups.sh'.format(args.kafka_dir),
        '--group {}'.format(args.group),
        '--describe'
    ]
    if args.zookeeper:
        params = params + ['--zookeeper', args.zookeeper]
    else:
        params = params + ['--new-consumer', '--bootstrap-server', args.bootstrap_server]

    cmd = subprocess.Popen(" ".join(params), shell=True, stdout=subprocess.PIPE)

    return [line for line in cmd.stdout]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--kafka-dir', default='/opt/kafka/', help='Kafka base directory', required=True)
    parser.add_argument('--group', help='Kafka group to check', required=True)
    parser.add_argument('--bootstrap-server', help='Which kafka to query. Used for new consumer')
    parser.add_argument('--zookeeper', help='Which zookeeper to query. Used for old consumer')
    parser.add_argument('--idb_host', help='Influxdb host')
    parser.add_argument('--idb_port', type=int, help='Influxdb port')
    parser.add_argument('--idb_user', help='Influxdb user')
    parser.add_argument('--idb_pass', help='Influxdb password')
    parser.add_argument('--idb_db', help='Influxdb database')
    args = parser.parse_args()
    assert args.zookeeper or args.bootstrap_server, "requires either --zookeeper or --bootstarp-server"
    assert args.idb_host and args.idb_port and args.idb_user and args.idb_pass and args.idb_db,\
        "You must provide all influxdb parameters: idb_host, idb_port, idb_user, idb_pass, idb_db"

    output = get_kafka(args)
    parsed_output = parse_output(output)
    json_data = to_influxdb_json(parsed_output)
    client = InfluxDBClient(args.idb_host, args.idb_port, args.idb_user, args.idb_pass, args.idb_db)
    client.write_points(json_data)
