import json
import pymongo
import backoff


class DataStore(object):
    def __init__(self):
        self.database = None

    def connect(self, conn):
        raise NotImplementedError()

    def store(self, data, topic):
        raise NotImplementedError()

    
class MongoDB(DataStore):
    @backoff.on_exception(backoff.expo,
                      pymongo.errors.ConnectionFailure,
                      max_value=30)
    def connect(self, conn):
        conn_info = conn.split(':')
        self.database = pymongo.MongoClient(conn_info[0], int(conn_info[1]))['cot']

    def store(self, data, topic):
        self.database[topic].insert(json.loads(data))



class DataStoreFactory(object):
    @staticmethod
    def create_datastore(datastore_type):
        if datastore_type == 'mongodb':
            return MongoDB()