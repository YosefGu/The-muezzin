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
from stt.convert_audio  import ConvertAudio

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
                self.add_mapping_text_field()

                for record in data:
                    metadata = record.value['metadata']
                    metadata = self.convert_string_to_datetime(metadata)
                    unique_id = self.generate_unique_id(metadata)
                    metadata['unique_id'] = unique_id

                    file = self.read_file(metadata['path'])
                    self.save_file_in_mongodb(file, unique_id)
                    doc_list.append(metadata)

                self.save_metadata_on_elasticsearch(doc_list)
                text_dict = self.transcriber(doc_list)
                self.add_text_to_elasticsearch(text_dict)

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

    def add_mapping_text_field(self):
        new_field = {
            "text" : {"type" : "text"}
        }
        elastic_conn.add_mapping(new_field)

    def save_metadata_on_elasticsearch(self, doc_list):
        self.elastic_conn.insert_data(doc_list)

    def transcriber(self, doc_lst):
        dict_data = {}
        for doc in doc_lst:
            text = ConvertAudio.speech_to_text(doc['path'])
            dict_data[doc['unique_id']] =  text
        return dict_data

    def read_file(self, path):
        try:
            with open(path, 'rb') as f:
                audio_data = f.read()
                my_logger.info("File read successfully")
                return audio_data
        except Exception as e:
            my_logger.error(f"Error reading file.\nPath:{path}\nError:{e}")

    def save_file_in_mongodb(self, file, unique_id):
        self.mongodb_client.save_file(file, unique_id)
        
    def add_text_to_elasticsearch(self, text_dict):
        for doc_id, text in text_dict.items():
            self.elastic_conn.update_doc_with_text(doc_id, text)
         


if __name__ == "__main__":
    InitialProcessing().run()