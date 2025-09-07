from kafka import KafkaProducer
import os
import json


class Publisher():

    def __init__(self):
        self.conn = KafkaProducer(
            bootstrap_servers=os.getenv('BOOTSTRAP_SERVER'),
            value_serializer=lambda x: json.dumps(x).encode('utf-8') 
        )

    def publish_data(self, topic, data):
        try:
            self.conn.send(topic, data)
            self.conn.flush()
            return {"Message" : "The data was published successfully."}
        except Exception as e:
            print("Error publishing.", str(e))
            return {"Error": str(e)}
            