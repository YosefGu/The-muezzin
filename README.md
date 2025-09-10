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


Stage 4 Classification
Goal - classify documents according to text data and words identified as suspicious.
The classification and scoring process was performed using ElasticSearch

Steps
1) Decrypt encrypted strings
2) Divide into words and word combinations - stored as different variables in 
    the state of the class instance.
3) Add relevant fields to the ElasticSearch index mapping
4) Add initial data for all documents in the index
5) Pull the documents from Elastic - 
    a special query that gives a score for each document according to the percentage of text matching the attached words. 
    Only documents that contain at least one word of the requested words - 
    will be returned. 
    Everything else will be aligned with the initial conditions we defined earlier, 
    they do not pose a danger at all.
6) Go through the documents we received - 
    define the score for each document according to the 
    Elastic calculation (a special formula is used to give a 
    match score to the words we searched for)
7) Calculate the average score for all selected documents
8) Define the threat according to the average - 
    any score that exceeds the average is defined as a high threat, 
    what is below the average is defined as a medium threat.
9) Update the data in Elastic - 
    update a large amount of data at 
    once using the - helpers.bulk function.