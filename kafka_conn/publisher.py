from kafka import KafkaProducer
import os
import json
from logger import Logger

my_logger = Logger.get_logger()

class Publisher():
    _kafka_producer = None

    def __get_kafka_publisher(cls):

        if cls._kafka_producer:
            my_logger.info("Retrive kafka producer connect successful.")
            return cls._kafka_producer
        
        cls._kafka_producer = KafkaProducer(
            bootstrap_servers=os.getenv('BOOTSTRAP_SERVER'),
            value_serializer=lambda x: json.dumps(x).encode('utf-8') 
        )
        my_logger.info("Kafka producer initially successful.")
        return cls._kafka_producer

    @classmethod
    def publish_data(cls, topic, data):
        try:
            producer = cls.__get_kafka_publisher(cls)
            producer.send(topic, data)
            producer.flush()
            my_logger.info(f"The data has been successfully published to topic: {topic}.")
        except Exception as e:
           my_logger.error(f"Error publishing data to topic: {topic}\nError: {str(e)}")
           return str(e)
            