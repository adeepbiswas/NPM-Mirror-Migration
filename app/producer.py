import os
import requests
import json
import tarfile
import zipfile
import re
import couchdb
import datetime
import queue
import threading
import concurrent.futures
from prometheus_client import start_http_server, Summary, Counter, Gauge
from confluent_kafka import Consumer, Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

KAFKA_TOPIC_NUM_PARTITIONS = 12
KAFKA_TOPIC_REPLICATION_FACTOR = 1
npm_mirror_path = '../../../../../../NPM/npm-repository-mirror'

print("Trying to connect to kafka.")

#creating kafka admin client and topics
# ac = AdminClient({"bootstrap.servers": "broker-npm:9092"})
# print("kafka connection successful")
 
# topic1 = NewTopic('mig-npm-changes', num_partitions=KAFKA_TOPIC_NUM_PARTITIONS, replication_factor=KAFKA_TOPIC_REPLICATION_FACTOR)
# topic2 = NewTopic('mig-skipped_changes', num_partitions=KAFKA_TOPIC_NUM_PARTITIONS, replication_factor=KAFKA_TOPIC_REPLICATION_FACTOR)
# topic3 = NewTopic('mig-run_logs', num_partitions=KAFKA_TOPIC_NUM_PARTITIONS, replication_factor=KAFKA_TOPIC_REPLICATION_FACTOR)
# fs = ac.create_topics([topic1, topic2, topic3])

# # Initialize Kafka producer
# kafka_producer = Producer({"bootstrap.servers": "broker-npm:9092"})

def process_package_dir(subdir):
    for file_name in os.listdir(subdir):
        if file_name.endswith('.json'):
            

def traverse_npm_mirror():
    obj = os.scandir(npm_mirror_path)

    for entry in obj :
        if entry.is_dir(): # or entry.is_file():
            subdir = os.path.join(npm_mirror_path, entry.name)
            print("Processing package - ", subdir)
            process_package_dir(subdir)
            
    obj.close()
    
if __name__ == '__main__':
    traverse_npm_mirror()