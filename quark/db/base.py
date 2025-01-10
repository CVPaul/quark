from pymongo import ASCENDING, MongoClient, ReplaceOne

class ClientBase:

    def __init__(self):
        self.conn = MongoClient('mongodb://localhost:27017/')
        self.db = self.conn['ailab']