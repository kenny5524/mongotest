import time
import boto3
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

AWS_ACCESS_KEY_ID = 'your_aws_access_key_id'  # replace with your AWS access key ID
AWS_SECRET_ACCESS_KEY = 'your_aws_secret_access_key'  # replace with your AWS secret access key
BUCKET_NAME = 'your_bucket_name'  # replace with your target S3 bucket name

# Create an S3 client
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

class LogFileHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if not event.is_directory and event.src_path.endswith('mongodb_stream.log'):
            print(f"Log file modified: {event.src_path}, uploading to S3.")
            with open(event.src_path, 'rb') as data:
                s3.upload_fileobj(data, BUCKET_NAME, 'mongodb_stream.log')
            print(f"Uploaded log file to S3: s3://{BUCKET_NAME}/mongodb_stream.log")

if __name__ == "__main__":
    path = '.'  # directory to watch, in this case the current directory
    event_handler = LogFileHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
