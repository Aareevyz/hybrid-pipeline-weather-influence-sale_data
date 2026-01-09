import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date
from datetime import datetime as date_time

## @params: [JOB_NAME]
## Start At Glue Context

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.sparkSession
job = Job(glueContext)
## End At Glue Context

## date for partitioning
current_date = date_time.now().strftime("%Y-%m-%d")

## ETL logic for gold layer data

# Extract: Read data directly from S3 (skip Glue Data Catalog)
# Fetch transformed weather data from silver layer (Parquet)
weather_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type = "s3",
    connection_options = {
        "paths": [f"s3://weather-influence-on-sales/silver/weather/date_key={current_date}/"],
        "recurse": True
    },
    format = "parquet"
)
# Fetch sales data from raw layer (CSV)
sales_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type = "s3",
    connection_options = {
        "paths": [f"s3://weather-influence-on-sales/raw/sales/date_key={current_date}/"],
        "recurse": True
    },
    format = "csv",
    format_options = {"withHeader": True, "separator": ","}
)
# Convert to DataFrame for use with Spark SQL
weather_df = weather_dyf.toDF()
sales_df = sales_dyf.toDF()

# Transform: Join weather and sales data on store_id and date_key

# Clean and select relevant columns from weather data
weather_cleaned = weather_df.withColumn(
    "weather_date",
    to_date(col("timestamp")) # Convert timestamp to date in format YYYY-MM-DD
).select(
    col("store_id").alias("weather_store_id"),
    col("weather_date"),
    col("temp_celsius").alias("temperature_celsius"),
    col("humidity_percent"),
    col("weather_main").alias("weather_condition"),
    col("weather_desc").alias("weather_description")   
)

# clean and select relevant columns from sales data
sales_cleaned = sales_df.withColumn(
    "sales_date",
    to_date(col("sale_date"), "yyyyMMdd HH:mm:ss") # Convert date_key to date in format YYYY-MM-DD
).select(
    col("store_id").alias("sales_store_id"),
    col("sales_date"),
    col("product_name"),
    col("units_sold"),
    col("sales_amount")
)

print(f"Weather count: {weather_cleaned.count()}")
print(f"Sales count: {sales_cleaned.count()}")

# Join datasets from wather and sales on store_id and date
# Use Inner Join for matching records only
gold_df = sales_cleaned.join(
    weather_cleaned,
    (sales_cleaned.sales_store_id == weather_cleaned.weather_store_id) &
    (sales_cleaned.sales_date == weather_cleaned.weather_date),
    "inner"
## select relevant columns for gold layer(
).select(
    sales_cleaned.sales_store_id.alias("store_id"),
    sales_cleaned.sales_date.alias("date_key"),
    "product_name",
    "units_sold",
    "sales_amount",
    "temperature_celsius",
    "humidity_percent",
    "weather_condition",
    "weather_description"
)

## add date column for visualize in table
final_gold_df = gold_df.withColumn("sale_date",col("date_key"))

print(f"Gold count:{final_gold_df.count()}")

# Convert back to DynamicFrame for Glue compatibility and loading to target
from awsglue.dynamicframe import DynamicFrame
gold_dyf = DynamicFrame.fromDF(final_gold_df, glueContext, "gold_dyf")

# Load: Write the gold layer data back to Glue Catalog in Parquet format
output_path = "s3://weather-influence-on-sales/gold/" # Replace with your S3 bucket path
glueContext.write_dynamic_frame.from_options(
    frame = gold_dyf,
    connection_type = "s3",
    connection_options = {
        "path": output_path,
        "partitionKeys": ["date_key"]
    },
    format = "parquet"
)

job.commit()