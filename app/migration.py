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
import shutil

MAX_SIZE = 10e+6
SUBDIRECTORY_HASH_LENGTH = 3
OLD_PACKAGE_VERSIONS_LIMIT = 5 #determines max how many package versions to keep
# REMOTE_PACKAGE_DIR = '../../../../../../NPM/npm-packages'
npm_mirror_path = './toy-data'
REMOTE_PACKAGE_DIR = 'npm-packages'
# npm_mirror_path = '../../../../../../NPM/npm-repository-mirror'
DATABASE_NAME = 'try-1'

# Specify the path to the .env file in the main directory
dotenv_path = '.env'
# Load environment variables from the .env file
load_dotenv(dotenv_path)

# Access the variables
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
db = None

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
    # if not os.path.exists(LOCAL_PACKAGE_DIR):
    #     os.mkdir(LOCAL_PACKAGE_DIR)

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
def size_filter_document_and_package(change, package_name, doc_path, tarball_path):
    saved = True
    if os.path.getsize(doc_path) > MAX_SIZE:
        os.remove(doc_path)
        log_message = f"Package JSON too large, removed" # - {change['seq']}"
        print(log_message)
        saved = False
        doc_path = None

    # Saving the tarball only if the JSON was saved
    if saved and os.path.exists(tarball_path):
        if os.path.getsize(tarball_path) > MAX_SIZE:
            os.remove(tarball_path)
            log_message = f"Tarball too large, removed" # - {change['seq']}"
            print(log_message)
            if doc_path:
                os.remove(doc_path)
                log_message = f"Corresponding JSON removed as well" # - {change['seq']}"
                print(log_message)
                doc_path = None
            saved = False
            tarball_path = None
            
        return doc_path, tarball_path
    
    os.remove(doc_path)
    print("json too big or tar file doesn't exist")
    return None, None

def get_zip_creation_time(zip_filename):
    # Get the creation time of the zip file
    zip_creation_time = os.path.getctime(zip_filename)
    return zip_creation_time

def extract_relative_path(full_path):
    # Split the full path into parts using the directory separator (e.g., / or \)
    path_parts = full_path.split(os.path.sep)

    # Find the index of the "npm-packages" directory
    npm_packages_index = path_parts.index("npm-packages")

    # Extract the path from "npm-packages" to the end
    relative_path = os.path.join(*path_parts[npm_packages_index:])

    return relative_path

def log_deletions(deleted_zip_path):
    deleted_zip_path = extract_relative_path(deleted_zip_path)
    filename = "deleted_zips.txt"
    try:
        # Open the file in append mode, creating it if it doesn't exist
        with open(filename, 'a') as file:
            # Write the data to the file followed by a newline character
            file.write(str(deleted_zip_path) + '\n')
    except Exception as e:
        print(f"An error occurred while writing to the file: {e}")

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
                log_deletions(oldest_zip_path)
                os.remove(oldest_zip_path)
                log_message = f"Too many package versions, Deleted the oldest zip file: {zip_file}"
                print(log_message)
                return
            
# Function to compress the downloaded JSON and tarball into a zip file and store it in remote directory
def compress_files(raw_package_name, package_name, revision_id, doc_path, tarball_path, change):
    package_dir = create_directory_structure(raw_package_name)
    
    # delete older package versions only if the difference in version count and modification count is 2
    # implying no older package versions were removed from the json
    package_versions_count = len(change['versions'].keys())
    package_modification_count = len(change['time'].keys())
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
        if tarball_path:
            zip_file.write(tarball_path, os.path.basename(tarball_path))
            os.remove(tarball_path)  # Remove the individual tar file from old directory after compression
            
    # modify zip creation time
    os.utime(zip_path, (os.path.getmtime(doc_path), os.path.getmtime(doc_path)))
    os.remove(doc_path)  # Remove the individual JSON file from old directory after compression

    
    log_message = f"Compressed zip saved in remote" # - {change['seq']}"
    print(log_message)
    
    return zip_path

# Function to save the processed change details in our own database
def store_change_details(change, zip_path):
    # Store the important details regarding the change in the local CouchDB database.
    package_name = change['_id']
    change_seq_id = None #change['seq']
    package_revision_id = change['_rev']
    package_latest_version = change['dist-tags']['latest']
    package_versions_count = len(change['versions'].keys())
    
    package_latest_authors = None
    package_latest_maintainers = None
    package_latest_dependencies = None
    if 'author' in change['versions'][package_latest_version].keys():
        package_latest_authors = change['versions'][package_latest_version]['author']
    if 'maintainers' in change['versions'][package_latest_version].keys():
        package_latest_maintainers = change['versions'][package_latest_version]['maintainers']
    if 'dependencies' in change['versions'][package_latest_version].keys():
        package_latest_dependencies = change['versions'][package_latest_version]['dependencies']
    
    package_modification_count = len(change['time'].keys())
    package_latest_change_time = change['time'][package_latest_version]
    package_distribution_tags = change['dist-tags']
    
    package_deleted = False
    change_keys = list(change.keys())
    if 'deleted' in change_keys:
        package_deleted = change['deleted']
        
    zip_path = extract_relative_path(zip_path)
    
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
    log_message = f"Change record added to database" # - {change['seq']}"
    print(log_message)
    
def process_change(change, tgz_file_path, json_file_path):
    raw_package_name = change['_id']
    
    log_message = f"Raw package name: {raw_package_name}"
    print(log_message)
    
    if "/" in raw_package_name:
        segments = raw_package_name.split("/")
        package_name = segments[-1]
    else:
        package_name = raw_package_name
    
    doc_path, tarball_path = size_filter_document_and_package(change, package_name, json_file_path, tgz_file_path)
    print("doc path - ", doc_path)
    
    if doc_path:
        zip_path = compress_files(raw_package_name, package_name, change['_rev'], doc_path, tarball_path, change)
        store_change_details(change, zip_path)

def delete_directory(path):
    try:
        shutil.rmtree(path)
        print(f"Directory '{path}' and its contents have been deleted.")
    except Exception as e:
        print(f"An error occurred while deleting '{path}': {e}")

def process_package_dir(subdir, package_name):
    for file_name in os.listdir(subdir):
        if file_name.endswith('.json'):
            json_file_path = os.path.join(subdir, file_name)
            try:
                print("JSON file path - ", json_file_path)
                
                tgz_file_path = None
                
                with open(json_file_path) as json_file: 
                    change = json.load(json_file)
                    
                # finding corresponding tarball path
                # print("keys-", change)
                # break
                if '_id' in change.keys(): ###
                    print("hello")
                    
                    package_version = change['dist-tags']['latest']
                    tgz_file_name = f'{package_name}-{package_version}.tgz'
                    tgz_file_path = os.path.join(subdir, tgz_file_name)
                    
                    process_change(change, tgz_file_path, json_file_path)
            except Exception as e:
                os.remove(json_file_path)
                print(f"An error occurred while processing '{json_file_pathpath}': {e}")
                
    delete_directory(subdir)
                

def traverse_npm_mirror():
    
    # obj = os.scandir(npm_mirror_path)

    # for i, entry in enumerate(obj) :
    #     if entry.is_dir(): # or entry.is_file():
    #         subdir = os.path.join(npm_mirror_path, entry.name)
    #         print("Processing package - ", subdir)
    #         process_package_dir(subdir, entry.name) #add error handling
            
    # obj.close()
    
    with os.scandir(npm_mirror_path) as obj:
        for i, entry in enumerate(tqdm(obj, desc="Processing packages")):
            if entry.is_dir():
                subdir = os.path.join(npm_mirror_path, entry.name)
                print("\nProcessing package - ", subdir)
                process_package_dir(subdir, entry.name)  # Add error handling if needed
    
if __name__ == '__main__':
    db = create_or_connect_db(server, DATABASE_NAME)
    traverse_npm_mirror()