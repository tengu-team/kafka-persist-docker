import os
import sys
import backoff
from kafka import KafkaConsumer, errors
from datastores import DataStoreFactory


def backoff_hdlr(details):
    print("Backing off {wait:0.1f} seconds afters {tries} tries "
          "calling function {func} with args {args} and kwargs "
          "{kwargs}".format(**details))


@backoff.on_exception(backoff.expo,
                      errors.KafkaError,
                      max_value=30,
                      on_backoff=backoff_hdlr)
def connect_kafka(kafka, group):
    return KafkaConsumer(bootstrap_servers=kafka,
                         group_id=group)


def main():
    kafka = os.environ['kafka'].split(' ')
    topics = os.environ['topics'].split(' ')
    group = os.environ['groupid']
    datastore_type = os.environ['datastore_type']
    datastore_conn = os.environ['datastore_conn']
    print(kafka)
    print(topics)
    print(group)
    print(datastore_type)
    print(datastore_conn)
    sys.stdout.flush()

    datastore = DataStoreFactory.create_datastore(datastore_type)
    datastore.connect(datastore_conn)
    print('CREATED DATASTORE CONNECTION')
    sys.stdout.flush()
    consumer = connect_kafka(kafka, group)
    print('KAFKA CONNECTED')
    sys.stdout.flush()
    consumer.subscribe(topics)
    print('KAFKA SUBSCRIBED')
    sys.stdout.flush()
    for msg in consumer:
        data = msg.value.decode('utf-8')
        datastore.store(data, msg.topic)


if __name__ == '__main__':
    main()
