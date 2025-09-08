from pymongo import MongoClient
import os
from gridfs import GridFS

class MongodbClient():
    _instance = None

    def __get_mongodb_instance(cls):
        if cls._instance:
            return cls._instance
        cls._instance = cls._initialize()
        return cls._instance
        # cls._mongo_client =  MongoClient(os.getenv('MONGODB_CONN'))
        # database = cls._mongo_client['muezzin']
        # fs = GridFS(database, collection='audio')

    def _initialize():
        client =  MongoClient(os.getenv('MONGODB_CONN'))
        db = client['muezzin']
        fs = GridFS(db, collection='audio')
        return {"client": client, "db": db, "fs": fs}

    # def __init__(self):
        # self.client = MongoClient(os.getenv('MONGODB_CONN'))
        # self.database = self.client['muezzin']
    #     self.fs = GridFS(self.database, collection='audio')

    
    def save_file(self, file, unique_id):
        file_id = self.fs.put(file, metadata={"unique_id": unique_id})
        return {"file_id": file_id}
    