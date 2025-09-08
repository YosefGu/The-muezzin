from pymongo import MongoClient
import os
from gridfs import GridFS

class MongodbClient():

    def __init__(self):
        self.client = MongoClient(os.getenv('MONGODB_CONN'))
        self.database = self.client['muezzin']
        self.fs = GridFS(self.database, collection='audio')

    
    def save_file(self, file, unique_id):
        file_id = self.fs.put(file, metadata={"unique_id": unique_id})
        return {"file_id": file_id}
    