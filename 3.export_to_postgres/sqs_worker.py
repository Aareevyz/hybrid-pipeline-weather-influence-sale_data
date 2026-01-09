## Polling script for SQS messages and triggering export to Postgres
import os
import json
import boto3
import time
from export_table import main as export_to_postgres
from dotenv import load_dotenv

# location of your .env file
script_dir = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(script_dir,'.env')

# Load environment variables from .env file
load_dotenv(dotenv_path)

QUEUE_URL = os.getenv('SQS_QUEUE_URL')  # SQS Queue URL from environment variable

def poll_sqs_and_export():

    if not QUEUE_URL:
        raise ValueError("SQS_QUEUE_URL environment variable not set.")
    
    sqs = boto3.client('sqs', region_name='ap-northeast-1')

    print("Starting SQS polling for a message...")

    while True:
        # Poll messages from SQS
        response = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10  # Long polling
        )

        messages = response.get('Messages', [])
        if not messages:
            print("No messages in queue. Waiting...")
            time.sleep(5)
            continue

        for msg in messages:
            body = json.loads(msg['Body'])
            print(f"Received message body: {body}")
            target_date = body.get('date_key')
            s3_prefix = f'gold/date_key={target_date}/'

            try:
                print(f"Processing message with S3 prefix: {s3_prefix}")
                # Call the export function with the specific S3 prefix
                export_to_postgres(specific_prefix=s3_prefix)

                # Delete message from queue after successful processing
                sqs.delete_message(
                    QueueUrl=QUEUE_URL,
                    ReceiptHandle=msg['ReceiptHandle']
                )
                print("Message processed and deleted from queue.")

            except Exception as e:
                print(f"Error processing message: {e}")
        
        time.sleep(2)  # Brief pause before next poll

if __name__ == "__main__":
    poll_sqs_and_export() 