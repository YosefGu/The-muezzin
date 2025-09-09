The Muezzin Project

Goal - changes as the project progresses

Steps
1) A service that reads files from a folder, 
    creates a json with metadata information and a path to the file 
    and publishes it in the Kafka service.

2) A service that tracks the data published in Kafka, 
    for each json saves the metadata in the Elasticsearch service 
    and the file content in MongoDB.

3) A service that takes audio files and converts them to text.

4) Saving the content of the audio files in Elasticsearch.


The structure of the classes and their relationships

Classes that manage connections to various services - Kafka, MongoDB, ElasticSearch.
Perform various operations -
Kafka - publishing and retrieving data.
MongoDB - saving data.
ElasticSearch - saving and updating data.

Classes that perform operations
1) Extraction - a class that reads files, 
    stores them and publishes them to Kafka.
2) Audioconversion - receives a path to a file, 
    converts the contents of the audio file and returns it.

Preprocessing - manages all operations and uses the various classes as needed
1) Extracts information from Kafka
2) Saves to MongoDB and ElasticSearch
3) Converts the files to text
4) Saves the text content to Elastic

A script folder containing executable files for each container we have uploaded.