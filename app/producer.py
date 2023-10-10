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

def process_package_dir(subdir, package_name):
    for file_name in os.listdir(subdir):
        if file_name.endswith('.json'):
            json_file_path = os.path.join(subdir, file_name)
            print("JSON file path - ", json_file_path)
            
            tgz_file_path = None
            
            with open(json_file_path) as json_file: ###
                change = json.load(json_file)
                
            # finding corresponding tarball path
            if 'doc' in change.keys(): ###
                package_version = change['doc']['dist-tags']['latest']
                tgz_file_name = f'{package_name}-{package_version}.tgz'
                tgz_file_path = os.path.join(subdir, tgz_file_name)
            
            change['old_tgz_file_path'] = tgz_file_path
            
            # send data to kafka topic
            try:
                kafka_producer.produce("mig-npm-changes", value=line)
                kafka_producer.flush()
                print("Change sent to Kafka stream")
                
                # Delete the JSON file after successfully producing data to Kafka
                os.remove(json_file_path)
                
            except Exception as e:
                if "Message size too large" in str(e) or \
                "MSG_SIZE_TOO_LARGE" in str(e):
                    log_message = f"Seq ID - {change['seq']} - Message size too large. Unable to produce message."
                    print(log_message)
                    kafka_producer.produce("mig-run_logs", value=log_message)
                else:
                    log_message = f"Seq ID - {change['seq']} - Error:{e}, change skipped."
                    print(log_message)
                    kafka_producer.produce("mig-run_logs", value=log_message)
                kafka_producer.produce("mig-skipped_changes", value=str(change['seq']))
                
                # Delete the JSON file after processing json file
                os.remove(json_file_path)

def traverse_npm_mirror():
    obj = os.scandir(npm_mirror_path)

    for entry in obj :
        if entry.is_dir(): # or entry.is_file():
            subdir = os.path.join(npm_mirror_path, entry.name)
            print("Processing package - ", subdir)
            process_package_dir(subdir, entry.name) #add error handling
            
    obj.close()
    
if __name__ == '__main__':
    traverse_npm_mirror()