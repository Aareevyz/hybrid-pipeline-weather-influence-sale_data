import boto3
import os

# --- Configuration ---
S3_BUCKET_NAME = 'weather-influence-on-sales'
S3_KEY = 'scripts/'  ## S3 Key (folder path) where the script will be uploaded

# Compute the local script path relative to this file so it works regardless of current working directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_SCRIPT_PATH = os.path.join(BASE_DIR, 'weather_etl_job_script.py')   # Local path of the script to upload

def upload_script_to_s3(bucket_name, s3_key, local_file_path):
    """Uploads a local script file to the specified S3 bucket and key."""
    s3 = boto3.client('s3')

    ## fetch file name from local path
    file_name = os.path.basename(local_file_path)
    s3_full_path = f"{s3_key}{file_name}"

    ##check the file exists locally
    if not os.path.isfile(local_file_path):
        print(f"ERROR: Local file '{local_file_path}' does not exist.")
        return

    try:
        s3.upload_file(local_file_path, bucket_name, s3_full_path)
        print(f"Script uploaded successfully to s3://{bucket_name}/{s3_full_path}")
    except Exception as e:
        print(f"Error uploading script to S3: {e}")

if __name__ == "__main__":
    upload_script_to_s3(S3_BUCKET_NAME, S3_KEY, LOCAL_SCRIPT_PATH)