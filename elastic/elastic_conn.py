from elasticsearch import Elasticsearch, helpers
import os
from logger import Logger

my_logger = Logger.get_logger()

es = Elasticsearch(os.getenv('ES_PATH'))
index_name = os.getenv('ES_INDEX')

def initialize():
    delete_index()
    create_index()
    add_mapping()
    my_logger.info("Elasticsearch connection initialization completed.")

def create_index():
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)
        my_logger.info("Index created successfully")


def delete_index():
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        my_logger.info("Index deleted successfully")

def add_mapping(): 
    mapping = {
        'properties': {
            'name': {
                'type': 'keyword',
            },
            'size': {
                'type': 'long',
            },
            'creation_date': {
                'type': 'date',
            },
            'path': {
                'type': 'keyword',
            },
            'unique_id': {
                'type': 'keyword',
            }
        }
    } 
    es.indices.put_mapping(index=index_name, body=mapping) 
    my_logger.info("Add mapping successfully")

def insert_data(doc_list):
    actions = [
        {
            "_index": index_name,
            "_source": doc
        }
        for doc in doc_list
    ]
    helpers.bulk(es, actions)
    es.indices.refresh(index=index_name) 
    my_logger.info("Metedate saved to elasticsearch successfully")


def update_doc_adding_text(doc_id, text):
    query = {
        "script": { 
            "source" : "ctx._source.text = params['text']",
            "params": {
                "text": text
            },
            "lang": "painless"
        },
        "query" : {"term" : {"unique_id" : doc_id}},   
    }
    response = es.update_by_query(
        index=index_name,
        body=query,
        wait_for_completion=True
    )
    if response['updated'] > 0:
        my_logger.info("Elasticsearch doc successfully updated.")
    else:
        my_logger.error(f"Error updating elasticsearch doc.\nResponse:{response}")

