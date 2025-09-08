from pymongo import MongoClient
import os
from gridfs import GridFS
from logger import Logger

my_logger = Logger.get_logger()

class MongodbClient():
    
    def __init__(self):
        self.client = MongoClient(os.getenv('MONGODB_CONN'))
        self.database = self.client['muezzin']
        self.fs = GridFS(self.database, collection='audio')
        my_logger.info("MongoDB connection completed successfully.")
    
    def save_file(self, file, unique_id):
        try:
            file_id = self.fs.put(file, metadata={"unique_id": unique_id})
            my_logger.info("Data saved successfully.")
            return {"file_id": file_id}
        except Exception as e:
            my_logger.error(f"Error saving data to mongodb.\nError:{e}")
            return str(e)