from dotenv import load_dotenv
load_dotenv()

import statistics
import os
from pathlib import Path
import sys
base_root = Path(__file__).resolve().parent.parent
sys.path.append(str(base_root))

from base64_handle import Base64Handle
from elastic import elastic_conn
from logger import Logger


my_logger = Logger.get_logger()

class Classifier():

    def __init__(self, 
            most_hostile_words=os.getenv('MOST_HOSTILE_WORDS'),
            hostile_words=os.getenv('HOSTILE_WORDS')
        ):
        self.most_hostile_words = most_hostile_words
        self.hostile_words = hostile_words
        self.expression_hostile = []
        self.expression_most_hostile = []

    def activate_classification(self):
        my_logger.info("Start classification actions.")
        try:
            self.__decode_text()
            self.__split_text()
            self.extract_expressiom()

            self.add_new_mapping_fields()
            self.adding_initial_values()

            docs = self.get_hostile_documents()

            updated_docs = self.__docs_classification(docs)
            elastic_conn.update_docs_using_original_id(updated_docs)

            my_logger.info("Finish classification actions.")
        except Exception as e:
            my_logger.error(f"An error occurred, classification failed.\nError:{e}")

    def __decode_text(self):
        self.most_hostile_words = Base64Handle.decode(self.most_hostile_words)
        self.hostile_words = Base64Handle.decode(self.hostile_words)

    def __split_text(self):
        self.most_hostile_words = self.most_hostile_words.split(',')
        self.hostile_words  =  self.hostile_words.split(',')

    def extract_expressiom(self):
        for word in self.hostile_words:
            if len(word.split()) > 1:
                self.expression_hostile.append(word)
        for word in self.most_hostile_words:
            if len(word.split()) > 1:
                self.expression_most_hostile.append(word)     

    def __create_hostile_query(self):
        should = []
        for word in self.hostile_words:
            should.append({"match" : {"text" : {"query" : word}}})
        for word in self.most_hostile_words:
            should.append({"match" : {"text" : {"query" : word, "boost" : 2}}})
        for expression in self.expression_hostile:
            should.append({"match_phrase" : {"text" :{"query" : expression}}})
        for expression in self.expression_most_hostile:
            should.append({"match_phrase" : {"text" : {"query" :expression, "boost" : 2}}})

        query = {
            "query": {
                "bool" : {
                    "should" : should,
                    "minimum_should_match" : 1
                }
            },
            "sort": [
                {
                "_score": {
                    "order": "asc"
                }
                }
  ]
        }
        return query

    def get_hostile_documents(self):
        query = self.__create_hostile_query()
        response = elastic_conn.search_by_custom_query(query)
        return response

    def add_new_mapping_fields(self):
        mapping = {
            "bds_percent": {"type" : "float"},
            "is_bds" : {"type" : "boolean"},
            "bds_threat_level" : {"type" : "keyword"}
        }

        elastic_conn.add_mapping(mapping)

    def adding_initial_values(self):
        query = {
            "query" : {"match_all" : {}},
            "script" : {
                "lang": "painless",
                "source" : "ctx._source.bds_percent = params.bds_percent; ctx._source.is_bds = params.is_bds; ctx._source.bds_threat_level = params.bds_threat_level;",
                "params": {
                    "bds_percent" : 0,
                    "is_bds" : False,
                    "bds_threat_level" : "none"
                },
               
           },
        }
        elastic_conn.update_docs_by_query(query)

    def __docs_classification(self, docs):
        docs_to_update = {}
        scores = []
        for doc in docs:
            scores.append(doc['_score'])
            docs_to_update[doc['_id']] = {"bds_percent" : doc['_score']}

        average = sum(scores) / len(scores)
        for doc in docs_to_update.values():
            if doc['bds_percent'] > average:
                doc['is_bds'] = True
                doc['bds_threat_level'] = 'hige'
            else:
                doc['bds_threat_level'] = 'medium'

        return docs_to_update
    

if __name__ == "__main__":
    Classifier().activate_classification()
