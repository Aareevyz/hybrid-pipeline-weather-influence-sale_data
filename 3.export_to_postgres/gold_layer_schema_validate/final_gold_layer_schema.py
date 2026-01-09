import  boto3 
from datetime import datetime

#connect to AWS Glue
glue = boto3.client('glue', region_name='ap-northeast-1') 

current_date = datetime.now().strftime("%Y-%m-%d")

def setup_weather_data_crawler():
    crawler_name = 'fact_table_crawler_gold_layer'
    s3_target_path = f's3://weather-influence-on-sales/gold/date={current_date}/' # S3 path where weather data is stored
    database_name = 'weather_db'
    table_prefix = 'fact_daily_sales_weather_'
    role_arn = 'arn:aws:iam::098131747502:role/weather_data_GlueServiceRole'  # Replace with your IAM role ARN

    try:
        glue.create_crawler(
            Name = crawler_name,
            Role = role_arn,
            DatabaseName = database_name,
            Description = 'Crawler for weather_structured data in silver layer',
            Targets={
                'S3Targets': [
                    {
                        'Path': s3_target_path
                    },
                ]
            },
            TablePrefix = table_prefix,
            ## Optional: Add a schema change policy to handle schema updates
            SchemaChangePolicy={
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
            }
        )
        print(f"Crawler '{crawler_name}' created successfully.")

        # Running the crawler
        glue.start_crawler(Name=crawler_name)
        print(f"Crawler '{crawler_name}' started successfully.")
    
    except glue.exceptions.AlreadyExistsException:
        print(f"Crawler '{crawler_name}' already exists. Starting the existing crawler.")
        glue.start_crawler(Name=crawler_name)
    except Exception as e:
        print(f"Error creating or starting crawler: {e}")   

if __name__ == "__main__":
    setup_weather_data_crawler()
    