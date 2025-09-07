from elasticsearch import Elasticsearch, helpers
import os

es = Elasticsearch(os.getenv('ES_PATH'))
index_name = os.getenv('ES_INDEX')

def initialize():
    delete_index()
    create_index()
    add_mapping()

def create_index():
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)

def delete_index():
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)

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