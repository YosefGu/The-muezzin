import os
from dotenv import load_dotenv
from pathlib import Path
import sys
from datetime import datetime

load_dotenv()

base_root = Path(__file__).resolve().parent.parent
sys.path.append(str(base_root))

from kafka_conn.publisher import Publisher
from logger import Logger

my_logger = Logger.get_logger()

class Extraction():

    def __init__(self):
        self.root_folder = os.getenv('ROOT_FOLDER')
    
    def run(self):
        try:
            for file_name in os.listdir(self.root_folder):
                json_result = self.__create_json_metadata(file_name)
                json_result["metadata"]["path"] = f'{self.root_folder}\{file_name}'
                self.__publish_to_kafka(json_result)
            my_logger.info("The metadata of all audio files has been published successfully.")
        except Exception as e:
            my_logger.error(f"Error publishing metadata to kafka.\nError:{e}")
            return str(e)
          
    def __create_json_metadata(self, file_name):
        meta_data = os.stat(f'{self.root_folder}/{file_name}')
        json_result = {
            "metadata": {
                "name" : file_name,
                "size": meta_data.st_size,
                "creation_date": str(datetime.fromtimestamp(meta_data.st_ctime))
            }
        }
        return json_result
    
    def __publish_to_kafka(self, json_result):
        return Publisher.publish_data(os.getenv('METADATA_TOPIC'), json_result)
        
if __name__ == "__main__":
    Extraction().run()