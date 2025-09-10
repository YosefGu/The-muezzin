from elasticsearch import Elasticsearch, helpers
import os
from logger import Logger

my_logger = Logger.get_logger()


es = Elasticsearch(os.getenv('ES_PATH'))
index_name = os.getenv('ES_INDEX')

def initialize():
    mapping = {
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
    __delete_index()
    __create_index()
    add_mapping(mapping)
    my_logger.info("Elasticsearch connection initialization completed.")

def __create_index():
    try:
        if not es.indices.exists(index=index_name):
            es.indices.create(index=index_name)
            my_logger.info("Index created successfully")
    except Exception as e:
        my_logger.error(f"Error creating new index.\nError:{e}")

def __delete_index():
    try:
        if es.indices.exists(index=index_name):
            es.indices.delete(index=index_name)
            my_logger.info("Index deleted successfully")
    except Exception as e:
        my_logger.error(f"Error deleting index.\nError:{e}")

def add_mapping(mapping):  
    try:
        es.indices.put_mapping(index=index_name, body={'properties' : mapping}) 
        my_logger.info("Mapping added successfully.")
    except Exception as e:
        my_logger.error(f"Error adding new field mapping.\nError:{e}")

def insert_data(doc_list):
    try:
        actions = [
            {
                "_index": index_name,
                "_source": doc
            }
            for doc in doc_list
        ]
        helpers.bulk(es, actions)
        es.indices.refresh(index=index_name) 
        my_logger.info("Data saved to elasticsearch successfully")
    except Exception as e:
        my_logger.error(f"Error adding saving data to elasticsearch.\nError:{e}")

def update_doc_with_text(doc_id, text):
    try:
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
    except Exception as e:
        my_logger.error(f"Error updating docs with text field.\nError:{e}")

def search_by_custom_query(query):
    try:
        response = es.search(index=index_name, body=query)
        my_logger.info("Search documents by custom query succeeded.")
        return response['hits']['hits']
    except Exception as e:
        my_logger.error(f"Search documents by custom query failed.\nError:{e}")

def update_docs_by_query(query):
    try:
        response = es.update_by_query(
            index=index_name,
            body=query,
            wait_for_completion=True
        )
        if response['updated'] > 0:
            my_logger.info("Elasticsearch docs successfully updated.")
        else:
            my_logger.error(f"Error updating elasticsearch docs.\nResponse:{response}")
    except Exception as e:
        my_logger.error(f"Error updating docs by custom query.\nError:{e}")

def update_docs_using_original_id(dict_data):
    try:
        actions = [
            {
                "_op_type": "update",
                "_index": index_name,
                "_id": doc_id,
                "doc": doc,
            } for doc_id, doc in dict_data.items()
        ]
        success, errors = helpers.bulk(es, actions)
        if errors:
            my_logger.error(f"Error updating.\nError:{e}")
        else:
            my_logger.info("Docs updated successfully.")
            es.indices.refresh(index=index_name)
    except Exception as e:
        my_logger.error(f"Failed to update by original ID.\nError:{e}")
    

