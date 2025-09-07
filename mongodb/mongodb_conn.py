from pymongo import MongoClient
import os
from gridfs import GridFS

class MongodbClient():

    def __init__(self):
        self.client = MongoClient(os.getenv('MONGODB_CONN'))
        self.database = self.client['muezzin']
        self.fs = GridFS(self.database)

    
    def save_data(self, file, file_name):
        metadata = 'metadate'
        file_id = self.fs.put(file, filename=file_name, **metadata, content_type="audio/mpeg")
        print(f"Audio file uploaded with ID: {file_id}")
    