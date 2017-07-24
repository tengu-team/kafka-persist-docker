import os
import json
import pymongo
import backoff
import pika
import signal
import sys


class DataStore(object):
    def __init__(self):
        self.datastore = None
        signal.signal(signal.SIGTERM, self.close)

    def close(self):
        raise NotImplementedError()

    def connect(self):
        raise NotImplementedError()

    def store(self, data, topic):
        raise NotImplementedError()


class MongoDB(DataStore):
    @backoff.on_exception(backoff.expo,
                          pymongo.errors.ConnectionFailure,
                          max_value=30)
    def connect(self):
        conn_info = os.environ['mongodb_conn'].split(':')
        self.datastore = pymongo.MongoClient(conn_info[0], int(conn_info[1]))['cot']

    def store(self, data, topic):
        self.datastore[topic].insert(json.loads(data))

    def close(self):
        if self.datastore:
            self.datastore.close()


class RabbitMQ(DataStore):
    def connect(self):
        credentials = pika.PlainCredentials(os.environ['rabbitmq_username'],
                                            os.environ['rabbitmq_password'])
        self.datastore = pika.BlockingConnection(
            pika.ConnectionParameters(host=os.environ['rabbitmq_host'],
                                      port=int(os.environ['rabbitmq_port']),
                                      virtual_host=os.environ['rabbitmq_vhost'],
                                      credentials=credentials))
        self.channel = self.datastore.channel()
        for topic in os.environ['topics'].split(' '):
            self.channel.queue_declare(queue=topic)

    def store(self, data, topic):
        self.channel.basic_publish(exchange='',
                                   routing_key=topic,
                                   body=json.dumps(data))

    def close(self):
        print('CLOSING RABBITMQ CONNECTION')
        sys.stdout.flush()
        if self.datastore:
            self.datastore.close()
        print('CONNECTION CLOSED')
        sys.stdout.flush()


class DataStoreFactory(object):
    @staticmethod
    def create_datastore(datastore_type):
        if datastore_type == 'mongodb':
            return MongoDB()
        elif datastore_type == 'rabbitmq':
            return RabbitMQ()
