import base64
from logger import Logger

my_logger = Logger.get_logger()

class Base64Handle():

    @classmethod
    def decode(self, encoded_str):
        try:
            decoded_str = base64.b64decode(encoded_str).decode('utf-8')
            my_logger.info("String encoded successful.")
            return decoded_str
        except Exception as e:
            my_logger.error(f"An error occurred, decoding string failed.\nError:{e}")
    