import hashlib
import sys
from pathlib import Path
import time
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()
base_root = Path(__file__).resolve().parent.parent
sys.path.append(str(base_root))

from kafka_conn.subscriber import Subscriber
from elastic import elastic_conn
from mongodb.mongodb_conn import MongodbClient 
from logger import Logger

my_logger = Logger.get_logger()


class InitialProcessing():

    def __init__(self):
        self.mongodb_client = MongodbClient()
        self.elastic_conn = elastic_conn


    def run(self):
        try:
            self.elastic_conn.initialize()
            while True:
                my_logger.info("Start preprocessing chunk of data (max 10 file)")
                doc_list = []
                data = self.pull_data_from_kafka()
                for record in data:
                    metadata = record.value['metadata']
                    metadata = self.convert_string_to_datetime(metadata)
                    unique_id = self.generate_unique_id(metadata)
                    metadata['unique_id'] = unique_id
                    self.save_data_on_mongodb(metadata['path'], unique_id)
                    doc_list.append(metadata)
                self.save_metadata_on_elasticsearch(doc_list)
                my_logger.info("Finish processing chunk of data")
                time.sleep(15)        
        except Exception as e:
            my_logger.error(f"Error ocorce: preprocessing process stopped.\nError:{e}")
            return str(e)

    # generaite unique id using file metadata
    def generate_unique_id(self, metadata):
        size = metadata['size']
        name = metadata['name'],
        creation_date = metadata['creation_date']

        unique_id = hashlib.md5(f'{size}{name}{creation_date}'.encode('utf-8')).hexdigest()
        return unique_id

    def convert_string_to_datetime(self, metadata):
        format_string = "%Y-%m-%d %H:%M:%S"
        datetime_object = datetime.strptime(metadata['creation_date'], format_string)
        metadata['creation_date'] = datetime_object
        return metadata
    
    def pull_data_from_kafka(self):
        return Subscriber.get_consumer_event()

    def save_metadata_on_elasticsearch(self, doc_list):
        self.elastic_conn.insert_data(doc_list)


    def save_data_on_mongodb(self, path, unique_id):
        try:
            with open(path, 'rb') as f:
                audio_data = f.read()
                self.mongodb_client.save_file(audio_data, unique_id)
        except Exception as e:
            print("Error: ", str(e))
            return {"Error: ", str(e)}   


if __name__ == "__main__":
    InitialProcessing().run()