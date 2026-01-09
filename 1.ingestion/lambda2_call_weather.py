import json
import requests
import boto3
import os

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Receive the list of stores from the previous Lambda function
    stores_id = event.get('store_id')
    lat = event.get('latitude')
    lon = event.get('longitude')
    date_key = event.get('date_key')

    # Prepare to call the OpenWeatherMap API
    api_key = os.getenv('OPENWEATHER_API_KEY') # Make sure to set this environment variable in your Lambda configuration

    # URL for OpenWeatherMap API
    url = f"http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}&units=metric"

    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad status codes
        weather_data = response.json()

        # define the S3 Path to store the weather data in partitioning format
        # Format: raw/weather/date_key=YYYY-MM-DD/store_id=STORE_ID/weather_data.json
        
        bucket_name = 'weather-influence-on-sales'
        s3_key = f"raw/weather/date_key={date_key}/store_id={stores_id}/weather_data.json"

        # Upload the weather data to S3
        s3.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json.dumps(weather_data),
            ContentType='application/json'
        )

        print(f"Successfully uploaded weather data for store {stores_id} to S3.")

        return {
                'store_id': stores_id,
                's3_key': s3_key,
                'status': 'success'
            }
    except Exception as e:
        print(f"Error fetching or uploading weather data for store {stores_id}: {str(e)}")
        return {
                'store_id': stores_id,
                'status': 'error',
                'message': str(e)
            }
    
    
        

