import threading
import json
from pymongo import MongoClient
from pymongo.errors import OperationFailure
from bson.json_util import dumps
from urllib.parse import quote_plus
import time
import pandas as pd
import boto3
from io import StringIO
import datetime
import logging
from dotenv import load_dotenv
import os



# Load the .env file
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')  # replace with your AWS access key ID
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')  # replace with your AWS secret access key
BUCKET_NAME = os.getenv('BUCKET_NAME')  # replace with your target S3 bucket name



# Create an S3 client
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

# Setup logging
logging.basicConfig(filename='mongodb_stream.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')
log = logging.getLogger()

def get_last_resume_token(collection_name, resume_token_collection):
    doc = resume_token_collection.find_one({'_id': collection_name})
    return doc['resume_token'] if doc else None

def store_last_resume_token(collection_name, resume_token, resume_token_collection):
    resume_token_collection.replace_one({'_id': collection_name}, {'_id': collection_name, 'resume_token': resume_token}, upsert=True)

def process_change_event(change):
    operation_type = change.get("operationType")
    full_document = change.get("fullDocument")
    if full_document.get('_id') is not None:
        full_document['_id'] = str(full_document['_id'])
    return {**{'operationType': operation_type}, **full_document}

def write_csv_file(data, collection_name):
    timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')  # current time in 'YYYYMMDDHHMMSS' format
    filename = f"{collection_name}/{collection_name}_{timestamp}.csv"
    df = pd.DataFrame(data)

    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        s3.put_object(Bucket=BUCKET_NAME, Key=filename, Body=csv_buffer.getvalue())
        log.info(f"Successfully written to {filename} in S3 bucket {BUCKET_NAME}")

    except Exception as e:
        log.error(f"Error writing to S3: {str(e)}")

def watch_collection(collection, resume_token_collection):
    pipeline = [{'$match': {'operationType': {'$in': ['insert', 'update', 'delete', 'replace']}}}]
    resume_token = get_last_resume_token(collection.name, resume_token_collection)
    resume_options = {'resume_after': resume_token} if resume_token else {}

    changes = []
    last_write_time = time.time()

    try:
        with collection.watch(pipeline, **resume_options) as stream:
            log.info(f"Change stream for collection '{collection.name}' started.")
            for change in stream:
                log.info(f"Change detected in collection '{collection.name}': {dumps(change)}")
                changes.append(process_change_event(change))
                store_last_resume_token(collection.name, change['_id'], resume_token_collection)
                if time.time() - last_write_time >= 300:  # 5 minutes
                    write_csv_file(changes, collection.name)
                    changes.clear()
                    last_write_time = time.time()
    except OperationFailure as e:
        log.error(f"Error setting up change stream for collection '{collection.name}': {e}")
    except Exception as e:
        log.error(f"Error watching collection: {str(e)}")

def print_change_stream():
    try:
        username = quote_plus(os.getenv('MONGO_USERNAME'))
        password = quote_plus(os.getenv('MONGO_PASSWORD'))
        uri = f"mongodb://{username}:{password}@mongo.libertyfrac.com:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=true"



        client = MongoClient(uri)
        db = client.ENGStageData  # Replace with your database name
        resume_token_collection = db['resume_tokens']  # Collection to store the resume tokens

        threads = []

        test_collectons = ['Sandbox_Material_boxes', 'resume_tokens', 'Sandbox_TimeTracker_StageData', 'Sandbox_TechSheet_StageData', 'Sandbox_TS_Row', 'Test']

        collections = [collection_name for collection_name in db.list_collection_names() if collection_name not in  test_collectons]

        for collection_name in collections:
            collection = db[collection_name]
            thread = threading.Thread(target=watch_collection, args=(collection, resume_token_collection))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        client.close()
    except Exception as e:
        log.error(f"Error in print_change_stream: {str(e)}")

print_change_stream()
