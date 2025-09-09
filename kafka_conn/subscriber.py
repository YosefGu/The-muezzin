from kafka import KafkaConsumer
import json
import os
from logger import Logger

my_logger = Logger.get_logger()

class Subscriber():
    _kafka_consumer = None

    def __get_kafka_consumer(cls):
        if cls._kafka_consumer:
            my_logger.info("Retrive kafka consumer connect successful.")
            return cls._kafka_consumer
        
        cls._kafka_consumer = KafkaConsumer(
            os.getenv('METADATA_TOPIC'),
            group_id = f'{os.getenv('METADATA_TOPIC')}-group',
            value_deserializer = lambda x: json.loads(x.decode('utf-8')),
            bootstrap_servers =  os.getenv('BOOTSTRAP_SERVER')
        )
        my_logger.info("Kafka consumer initially successful.")
        return cls._kafka_consumer

    @classmethod
    def get_consumer_event(cls):
        try:
            consumer = cls.__get_kafka_consumer(cls)
            while True:
                data = consumer.poll(timeout_ms=5000, max_records=10)
                list_data = []
                for record in data.values():
                    list_data.extend(record)
                if list_data:
                    my_logger.info(f"The data was successfully pulled from Kafka.")
                    return list_data
        except Exception as e:
            my_logger.error(f"Error pulling data from kafka.\nError: {str(e)}")
            return str(e)
