import os
import boto3
import pandas as pd
import io
import uuid
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text

## Load environment variables from .env file
script_dir = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(script_dir,'.env')

#load .env variables
load_dotenv(dotenv_path)

## 1. Configuration
# Date for S3 path
CURRENT_DATE = '2026-01-06'
# AWS S3 Configuration
S3_BUCKET = 'weather-influence-on-sales'
S3_PREFIX = f'gold/date_key={CURRENT_DATE}/'

# Local PostgreSQL Configuration
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = '5432'
DB_NAME = 'weather_sales_db'
TABLE_NAME = 'fact_daily_sales_weather'


def get_s3_parquet_dataframe(s3_client, bucket, prefix):
    """List parquet files under prefix and concatenate them into a single DataFrame."""
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.parquet')]

    if not files:
        raise FileNotFoundError("No parquet files found in the specified S3 path.")

    tables = []
    for file_key in files:
        obj = s3_client.get_object(Bucket=bucket, Key=file_key)
        data = obj['Body'].read()
        tables.append(pd.read_parquet(io.BytesIO(data)))

    df = pd.concat(tables, ignore_index=True)
    df.columns = [col.lower() for col in df.columns]
    return df


def ensure_required_columns(engine, table_name, df):
    """Ensure the dataframe contains any NOT NULL columns (with no default) from the target table.
    - For integer types, generate sequential ids starting after the current max in the table.
    - For other types, generate UUID strings.
    Returns the updated DataFrame.
    """
    inspector = inspect(engine)
    cols = inspector.get_columns(table_name)

    missing_cols = []
    for col in cols:
        name = col['name']
        nullable = col.get('nullable', True)
        default = col.get('default', None)
        if (not nullable) and (default is None) and (name not in df.columns):
            missing_cols.append(col)

    if not missing_cols:
        return df

    # We'll fetch max values for integer columns if needed
    with engine.connect() as conn:
        for col in missing_cols:
            name = col['name']
            coltype = col['type']
            coltype_str = str(coltype).upper()
            if 'INT' in coltype_str:
                # get current max
                try:
                    res = conn.execute(text(f"SELECT MAX({name}) FROM {table_name}"))
                    maxv = res.scalar()
                except Exception:
                    maxv = None
                start = (maxv or 0) + 1
                df[name] = range(start, start + len(df))
            else:
                df[name] = [str(uuid.uuid4()) for _ in range(len(df))]

    return df


def export_df_to_db(engine, table_name, df):
    """Export dataframe to DB after ensuring required columns."""
    df = ensure_required_columns(engine, table_name, df)
    df.to_sql(table_name, engine, if_exists='append', index=False)


def main(specific_prefix=None):
    print("Connecting to S3 ...")
    s3 = boto3.client('s3')

    # if specific_prefix is not provided, use default
    prefix = specific_prefix if specific_prefix else S3_PREFIX

    try:
        # use prefix to read data from S3
        gold_df = get_s3_parquet_dataframe(s3, S3_BUCKET, prefix)
        print(f"Total records read from S3: {len(gold_df)}")
    except FileNotFoundError as e:
        print(e)
        return

    print("Connecting to PostgreSQL ...")
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    try:
        export_df_to_db(engine, TABLE_NAME, gold_df)
        print("Data successfully exported to PostgreSQL.")
    except Exception as e:
        print(f"Error exporting data to PostgreSQL: {e}")

    print("Process Completed!.")


if __name__ == '__main__':
    main()
