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
from tqdm import tqdm
from dotenv import load_dotenv

KAFKA_TOPIC_NUM_PARTITIONS = 12
KAFKA_TOPIC_REPLICATION_FACTOR = 1
npm_mirror_path = './toy-data'
# npm_mirror_path = '../../../../../../NPM/npm-repository-mirror'
DATABASE_NAME = 'npm-mirror'

# Specify the path to the .env file in the main directory
dotenv_path = '.env'
# Load environment variables from the .env file
load_dotenv(dotenv_path)

# Access the variables
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Establish Couchdb server connection
server = couchdb.Server('http://{user}:{password}@localhost:5984/'.format(user=DB_USER, password=DB_PASSWORD))

# Function to create or connect to our own CouchDB database
def create_or_connect_db(server, database_name):
    try:
        db = server.create(database_name)
        log_message = f"Created new database: {database_name}"
        print(log_message)
    except couchdb.http.PreconditionFailed:
        db = server[database_name]
        log_message = f"Connected to existing database: {database_name}"
        print(log_message)
    return db

# Function to create temporary local directory and organized remote (NAS) directory structure
def create_directory_structure(package_name):
    #Create parent directory in local and remote storage system
    if not os.path.exists(REMOTE_PACKAGE_DIR):
        os.mkdir(REMOTE_PACKAGE_DIR)
    if not os.path.exists(LOCAL_PACKAGE_DIR):
        os.mkdir(LOCAL_PACKAGE_DIR)

    # Create subdirectories based on first 3 characters of the package name to add heirarchy
    if len(package_name) >= SUBDIRECTORY_HASH_LENGTH:
        first_char = package_name[0:SUBDIRECTORY_HASH_LENGTH].upper()
    else:
        first_char = package_name[0].upper()
    # first_char = package_name[0].upper()
    alpha_dir = os.path.join(REMOTE_PACKAGE_DIR, first_char)
    if not os.path.exists(alpha_dir):
        os.mkdir(alpha_dir)

    # Create subdirectory for each package in the corresponding alphabet directory
    if '/' in package_name:
        package_dir = alpha_dir
        segments = package_name.split("/")
        for segment in segments:
            package_dir = os.path.join(package_dir, segment)
            if not os.path.exists(package_dir):
                os.mkdir(package_dir)
    else:
        package_dir = os.path.join(alpha_dir, package_name)
        if not os.path.exists(package_dir):
            os.mkdir(package_dir)

    return package_dir

# Function to download package JSON and tarball corresponding to each change
def download_document_and_package(change, package_name):
    doc = change.get('doc')
    if doc:
        # creating directory in local storage for temporary downlaod and compression 
        # before moving to remote directory (for faster transfers)
        package_dir = LOCAL_PACKAGE_DIR
        if not os.path.exists(LOCAL_PACKAGE_DIR):
            os.mkdir(LOCAL_PACKAGE_DIR)
        
        saved = True

        # Save the document as a JSON file in temporary local directory
        doc_filename = f"{package_name}_doc.json"
        doc_path = os.path.join(package_dir, doc_filename)
        
        with open(doc_path, 'w') as doc_file:
            json.dump(doc, doc_file)
            log_message = f"Saved package JSON - {change['seq']}"
            print(log_message)  
        if os.path.getsize(doc_path) > MAX_SIZE:
            os.remove(doc_path)
            log_message = f"Package JSON too large, removed - {change['seq']}"
            print(log_message)
            saved = False
            doc_path = None

        # Saving the tarball only if the JSON was saved
        if saved:
            # Save the updated (latest) package as a tar file in temporary local directory
            latest = doc['dist-tags']['latest']
            tarball_url = doc['versions'][latest]['dist']['tarball']
            tarball_filename = f"{package_name}_package.tgz"
            tarball_path = os.path.join(package_dir, tarball_filename)
            
            response = requests.get(tarball_url)
            if response.status_code == 200:
                with open(tarball_path, 'wb') as tarball_file:
                    tarball_file.write(response.content)
                log_message = f"Saved Tar file - {change['seq']}"
                print(log_message)
                
                if os.path.getsize(tarball_path) > MAX_SIZE:
                    os.remove(tarball_path)
                    log_message = f"Tarball too large, removed - {change['seq']}"
                    print(log_message)
                    kafka_producer.produce("run_logs", value=log_message)
                    kafka_producer.flush()
                    if doc_path:
                        os.remove(doc_path)
                        log_message = f"Corresponding JSON removed as well - {change['seq']}"
                        print(log_message)
                        doc_path = None
                    saved = False
                    tarball_path = None
            else:
                if doc_path:
                    os.remove(doc_path)
                    log_message = f"Corresponding JSON removed as well - {change['seq']}"
                    print(log_message)
                    doc_path = None
                saved = False
                tarball_path = None    
            return doc_path, tarball_path
    return None, None

def get_zip_creation_time(zip_filename):
    # Get the creation time of the zip file
    zip_creation_time = os.path.getctime(zip_filename)
    return zip_creation_time

# deletes the oldest zip version if there are already 3 or more versions present unless
# the next zip has a deletion type change
def delete_oldest_zip(directory):
    zip_files = [file for file in os.listdir(directory) if file.lower().endswith('.zip')]

    if len(zip_files) >= OLD_PACKAGE_VERSIONS_LIMIT:
        zip_file_times = [(zip_file, get_zip_creation_time(os.path.join(directory, zip_file))) for zip_file in zip_files]
        
        # Sort the list of zip files by creation time in ascending order
        zip_file_times.sort(key=lambda x: x[1])
        
        # Iterate through the sorted list
        for i in range(len(zip_file_times) - 1):
            zip_file, creation_time = zip_file_times[i]
            next_zip_file, next_creation_time = zip_file_times[i + 1]

            # Check if the next zip file has 'Deletion' in its name
            if not re.search(r'Deleted', next_zip_file, re.IGNORECASE):
                oldest_zip_path = os.path.join(directory, zip_file)
                os.remove(oldest_zip_path)
                log_message = f"Too many package versions, Deleted the oldest zip file: {zip_file}"
                print(log_message)
                return
            
# Function to compress the downloaded JSON and tarball into a zip file and store it in remote directory
def compress_files(raw_package_name, package_name, revision_id, doc_path, tarball_path, change):
    package_dir = create_directory_structure(raw_package_name)
    
    # delete older package versions only if the difference in version count and modification count is 2
    # implying no older package versions were removed from the json
    package_versions_count = len(change['doc']['versions'].keys())
    package_modification_count = len(change['doc']['time'].keys())
    if (package_modification_count - package_versions_count ) == 2:
        delete_oldest_zip(package_dir)
    
    package_deleted = False
    change_keys = list(change.keys())
    if 'deleted' in change_keys:
        package_deleted = change['deleted']
    
    if package_deleted:
        compressed_filename = f"Deleted-{package_name}_{revision_id}.zip"
    else:
        compressed_filename = f"{package_name}_{revision_id}.zip"
        
    zip_path = os.path.join(package_dir, compressed_filename)
    
    with zipfile.ZipFile(zip_path, 'w') as zip_file:
        if doc_path:
            zip_file.write(doc_path, os.path.basename(doc_path))
            os.remove(doc_path)  # Remove the individual JSON file from local (temp) directory after compression
        if tarball_path:
            zip_file.write(tarball_path, os.path.basename(tarball_path))
            os.remove(tarball_path)  # Remove the individual tar file from local (temp) directory after compression
    log_message = f"Compressed zip saved in remote - {change['seq']}"
    print(log_message)
    
    return zip_path

# Function to save the processed change details in our own database
def store_change_details(change, db, zip_path):
    # Store the important details regarding the change in the local CouchDB database.
    package_name = change['id']
    change_seq_id = change['seq']
    package_revision_id = change['doc']['_rev']
    package_latest_version = change['doc']['dist-tags']['latest']
    package_versions_count = len(change['doc']['versions'].keys())
    
    package_latest_authors = None
    package_latest_maintainers = None
    package_latest_dependencies = None
    if 'author' in change['doc']['versions'][package_latest_version].keys():
        package_latest_authors = change['doc']['versions'][package_latest_version]['author']
    if 'maintainers' in change['doc']['versions'][package_latest_version].keys():
        package_latest_maintainers = change['doc']['versions'][package_latest_version]['maintainers']
    if 'dependencies' in change['doc']['versions'][package_latest_version].keys():
        package_latest_dependencies = change['doc']['versions'][package_latest_version]['dependencies']
    
    package_modification_count = len(change['doc']['time'].keys())
    package_latest_change_time = change['doc']['time'][package_latest_version]
    package_distribution_tags = change['doc']['dist-tags']
    
    package_deleted = False
    change_keys = list(change.keys())
    if 'deleted' in change_keys:
        package_deleted = change['deleted']
    
    data = {
        'package_name': package_name, 
        'change_seq_id': change_seq_id,
        'package_revision_id': package_revision_id,
        'package_latest_version': package_latest_version,
        'package_versions_count': package_versions_count,
        'package_modification_count': package_modification_count,
        'package_latest_change_time': package_latest_change_time,
        'package_latest_authors': package_latest_authors,
        'package_latest_maintainers': package_latest_maintainers,
        'package_latest_dependencies': package_latest_dependencies,
        'change_save_path': zip_path,
        'package_deleted' : package_deleted,
        'package_distribution_tags': package_distribution_tags
    }
    db.save(data)
    log_message = f"Change record added to database - {change['seq']}"
    print(log_message)

def process_package_dir(subdir, package_name):
    for file_name in os.listdir(subdir):
        if file_name.endswith('.json'):
            json_file_path = os.path.join(subdir, file_name)
            # print("JSON file path - ", json_file_path)
            
            tgz_file_path = None
            
            with open(json_file_path) as json_file: ###
                change = json.load(json_file)
                
            # finding corresponding tarball path
            if 'doc' in change.keys(): ###
                package_version = change['doc']['dist-tags']['latest']
                tgz_file_name = f'{package_name}-{package_version}.tgz'
                tgz_file_path = os.path.join(subdir, tgz_file_name)
            
            change['old_tgz_file_path'] = tgz_file_path
                
            # Delete the JSON file after processing json file
            # os.remove(json_file_path)

def traverse_npm_mirror():
    
    # obj = os.scandir(npm_mirror_path)

    # for i, entry in enumerate(obj) :
    #     if entry.is_dir(): # or entry.is_file():
    #         subdir = os.path.join(npm_mirror_path, entry.name)
    #         print("Processing package - ", subdir)
    #         process_package_dir(subdir, entry.name) #add error handling
            
    # obj.close()
    
    db = create_or_connect_db(server, DATABASE_NAME)
    
    with os.scandir(npm_mirror_path) as obj:
        for i, entry in enumerate(tqdm(obj, desc="Processing packages")):
            if entry.is_dir():
                subdir = os.path.join(npm_mirror_path, entry.name)
                print("\nProcessing package - ", subdir)
                process_package_dir(subdir, entry.name)  # Add error handling if needed
    
if __name__ == '__main__':
    traverse_npm_mirror()