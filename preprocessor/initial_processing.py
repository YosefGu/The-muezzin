import hashlib
import sys
from pathlib import Path
import time
import os
from dotenv import load_dotenv
load_dotenv()
base_root = Path(__file__).resolve().parent.parent
sys.path.append(str(base_root))

from kafka_conn.subscriber import Subscriber


class InitialProcessing():

    def __init__(self):
        self.kafka_conn = Subscriber(os.getenv('METADATA_TOPIC'))
        self.run()

    def run(self):
        try:
            while True:
                data = self.pull_data_from_kafka()
                if data:
                    metadata = data.value['metadata']
                    unique_id = self.generate_unique_id(metadata)

                time.sleep(15)

                
        except Exception as e:
            print("Error: ", str(e))
            return {"Error: " : str(e)}

    def generate_unique_id(self, metadata):
        size = metadata['size']
        name = metadata['name'],
        creation_date = metadata['creation_date']

        unique_id = hashlib.md5(f'{size}{name}{creation_date}'.encode('utf-8')).hexdigest()
        return unique_id

    def pull_data_from_kafka(self):
        return self.kafka_conn.get_consumer_event()

    def save_metadata_on_elasticsearch(self, metadata):
        pass

    def save_data_on_mongodb(self, data):
        pass


if __name__ == "__main__":
    InitialProcessing()