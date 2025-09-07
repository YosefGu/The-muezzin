from kafka import KafkaConsumer
import json
import os

class Subscriber():

    def __init__(self, topic):
        self.topic = topic
        self.conn = KafkaConsumer(
            self.topic,
            group_id = f'{self.topic}-group',
            value_deserializer = lambda x: json.loads(x.decode('utf-8')),
            bootstrap_servers =  os.getenv('BOOTSTRAP_SERVER')
        )

    def get_consumer_event(self):
        data = self.conn.poll(timeout_ms=5000, max_records=1)
        list_data = []
        for record in data.values():
            list_data.extend(record)
        return list_data[0]

