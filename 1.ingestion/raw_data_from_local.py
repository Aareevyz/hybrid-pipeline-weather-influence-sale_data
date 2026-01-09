import hashlib
import boto3
import os
from botocore.exceptions import NoCredentialsError
from datetime import datetime, timedelta

# --- Configuration ---
S3_BUCKET_NAME = 'weather-influence-on-sales'  ## Input your S3 Bucket Name here
DATA_DATE = (datetime.now()+timedelta(days=3)).strftime('%Y-%m-%d')   # Date for partitioning data in S3
LOCAL_DIR = './0.data_src'  # Local Directory that contains files to upload
REGION_NAME = 'ap-northeast-1'  # AWS Region

# --- File Definitions ---
# define the files to upload with their respective S3 keys
files_to_upload = [
    {
        'local_file': 'dim_store.csv',             # Path for Dimension Data (no Partition Key)
        's3_key': 'raw/master/dim_store.csv'
    },
    {
        'local_file': 'dim_product.csv',             # Path for Dimension Data (no Partition Key)
        's3_key': 'raw/master/dim_product.csv'
    },    
    {
        'local_file': f'sales_transaction_data_{DATA_DATE.replace("-", "")}.csv',               # Path for Fact Data (with Partition Key)
        's3_key': f'raw/sales/date_key={DATA_DATE}/sales_data_{DATA_DATE.replace("-", "")}.csv' # S3 Key: raw/sales/date_key=2025-12-01/sales_data_20251201.csv
    }
]

# --- Function to Upload ---

def create_s3_bucket_if_not_exists(bucket_name, region_name):
    """Creates an S3 bucket if it does not already exist."""
    s3 = boto3.client('s3', region_name=region_name)

    try:
        s3.head_bucket(Bucket=bucket_name)  # Check if bucket exists
        print(f"S3 Bucket '{bucket_name}' already exists.")

    except s3.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']

        if error_code == '404':  # Bucket does not exist
            print(f"Creating S3 Bucket: {bucket_name}")
            try:
                if region_name == 'us-east-1':
                    s3.create_bucket(Bucket=bucket_name)
                else:
                    s3.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': region_name}
                    )
                print(f"S3 Bucket '{bucket_name}' created successfully.")
            except Exception as create_error:
                print(f"Error creating S3 bucket: {create_error}")
                raise

        elif error_code == '403':  # Forbidden, bucket exists but not owned by you
            print(f"ERROR: S3 Bucket '{bucket_name}' already exists and is owned by another account.")

        else:
            raise e


def get_s3_object_metadata(bucket_name, s3_key):
    """Retrieves ETag and Size of an object in S3, if it exists."""
    s3 = boto3.client('s3')
    try:
        #  use head_object instead of get_object to avoid downloading the file
        response = s3.head_object(Bucket=bucket_name, Key=s3_key)
        return response['ETag'].strip('"'), response['ContentLength']
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':  # 404 Error: Object not found
            return None, None
        else:
            raise # Re-raise other unexpected errors

def calculate_local_md5(local_file_path):
    """Calculates the MD5 hash of the local file (equivalent to S3 ETag for small files)."""
    # NOTE: for files uploaded in a single part, S3 ETag is simply the MD5 hash of the file content
    hash_md5 = hashlib.md5()
    with open(local_file_path, "rb") as f:
        # read file in chunks to avoid memory issues with large files
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()
    

def upload_to_s3(local_file_path, bucket_name, s3_key):
    """Uploads a file to a specified S3 path, skipping if ETag matches."""
    s3 = boto3.client('s3')
    
    try:
        if not os.path.exists(local_file_path):   #inspect local file path
            print(f"ERROR: Local file not found at {local_file_path}")
            return False

        s3_etag, s3_size = get_s3_object_metadata(bucket_name, s3_key)    # 2. check metadata on S3 (if file exists)

        if s3_etag: # 3. calculate local file MD5 (hash)
            local_md5 = calculate_local_md5(local_file_path)
            local_size = os.path.getsize(local_file_path)
            
            if local_md5 == s3_etag and local_size == s3_size: # 4. compare local MD5 with S3 ETag
                print(f"SKIP: File {s3_key} already exists on S3 and content hash matches. Skipping upload.")
                return True
            else:
                print(f"UPDATE: File {s3_key} content has changed. Proceeding with upload...")

        print(f"Uploading {local_file_path} to s3://{bucket_name}/{s3_key}")   
        s3.upload_file(local_file_path, bucket_name, s3_key)   # 5. if file doesn't exist on S3 or content has changed, upload the file
        print("Upload successful.")
        return True
        
    except NoCredentialsError:
        print("ERROR: AWS credentials not found. Please configure your credentials.")
        return False
    except Exception as e:
        print(f"An error occurred during upload: {e}")
        return False


def main():
    """ Main function to execute the ingestion process """

    # 0. Ensure S3 Bucket exists
    create_s3_bucket_if_not_exists(S3_BUCKET_NAME, REGION_NAME)
    print("\n--- S3 BUCKET CHECK COMPLETE ---\n")
    
    # 1. Inspect and create local directory if it doesn't exist
    if not os.path.exists(LOCAL_DIR):
        print(f"Creating local directory: {LOCAL_DIR}")
        os.makedirs(LOCAL_DIR)
    else:
        print(f"Local directory exists: {LOCAL_DIR}")
    
    # start uploading files to S3
    print("\n--- STARTING INGESTION PROCESS ---")
    all_success = True
    for file_info in files_to_upload:
        local_path = os.path.join(LOCAL_DIR, file_info['local_file'])
        
        # NOTE: we assume the local files are already present in LOCAL_DIR
        if not upload_to_s3(local_path, S3_BUCKET_NAME, file_info['s3_key']):
            all_success = False
            break
            
    if all_success:
        print("\n--- INGESTION COMPLETE ---")
        print(f"All files for DATE {DATA_DATE} successfully uploaded to S3 Bucket {S3_BUCKET_NAME}.")


if __name__ == "__main__":
    main()  