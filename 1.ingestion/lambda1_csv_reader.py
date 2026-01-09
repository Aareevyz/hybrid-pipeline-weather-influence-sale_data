import boto3
import csv
import io
from botocore.exceptions import NoCredentialsError
from datetime import datetime

s3 = boto3.client('s3')

Bucket_name = 'weather-influence-on-sales'
Store_key = 'raw/master/dim_store.csv'

def lambda_handler(event, context):
    #Receive the date
    process_date = event.get('process_date', datetime.now().strftime('%Y-%m-%d'))
    try:
        csv_object = s3.get_object(Bucket=Bucket_name, Key=Store_key)
        csv_content = csv_object['Body'].read().decode('utf-8-sig') #decode to utf-8 
        # Use DictReader for read csv and transform to python dictionary
        reader = csv.DictReader(io.StringIO(csv_content))
        
        stores_list = []
        for row in reader:
            stores_list.append({
                'store_id': row['store_id'],
                'latitude': float(row['latitude']),
                'longitude': float(row['longitude']),
                'date_key': process_date
            }) 
        return {
            'stores': stores_list
        }
    except Exception as e:
        print(f"Error processing dim_store.csv: {e}")
        raise e