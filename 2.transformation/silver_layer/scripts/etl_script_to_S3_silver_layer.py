#This script is used to transform raw weather data from S3 JSON files into a structured Parquet format in the S3 silver layer.
## Place this code into Glue Script Editor
import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import col, to_timestamp, from_unixtime, when, lit
from datetime import datetime

## @params: [JOB_NAME]
## Start At Glue Context 

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.sparkSession
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## End At Glue Context

# ETL logic for weather data 
current_date = datetime.now().strftime("%Y-%m-%d")  
input_path = f"s3://weather-influence-on-sales/raw/weather/date_key={current_date}/"

# Extract: the datacatalog table created by the crawler
datasource = glueContext.create_dynamic_frame.options(
    format_options={"multiline": False}, # Adjust based on your JSON structure
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [input_path],
        "recurse": True  # to include all subfolders
    }
)

# Transform: Explode the nested JSON structure to flatten the data
## Select the relevant fields and cast data types
flattened_data = datasource.resolveChoice(
    specs = [
        ('main.temp', 'cast:double'), 
        ('main.humidity', 'cast:int')
    ]
)

## change from DynamicFrame to DataFrame 
df = datasource.toDF()

# Fetch data from Array and Struct directly by Spark SQL
df_final = df.select(
    col("store_id"),
    col("date_key"),
    col("main.temp").alias("temp_celsius"),
    col("main.humidity").alias("humidity_percent"),
    col("wind.speed").alias("wind_speed"),
    col("weather").getItem(0).getField("main").alias("weather_main"),
    col("weather").getItem(0).getField("description").alias("weather_desc"),
    # Convert epoch (seconds OR milliseconds) into a Spark timestamp (UTC)
    when(col("dt") > lit(1_000_000_000_000),
         to_timestamp(from_unixtime((col("dt")/1000)))
    ).otherwise(
         to_timestamp(from_unixtime(col("dt")))
    ).alias("timestamp")
)

## Change to DynamicFrame from write into S3
selected_data = DynamicFrame.fromDF(df_final, glueContext, "selected_data")

# Load: Write the transformed data back to S3 in Parquet format, partitioned by date_key
output_path = "s3://weather-influence-on-sales/silver/weather/"

glueContext.write_dynamic_frame.from_options(
    frame = selected_data,
    connection_type = "s3",
    connection_options ={
        "path": output_path,
        "partitionKeys": ["date_key"]
    },
    format = "parquet"
)
job.commit()