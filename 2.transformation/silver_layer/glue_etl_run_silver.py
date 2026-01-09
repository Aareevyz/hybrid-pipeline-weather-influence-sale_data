## This script creates and runs an AWS Glue ETL job for processing weather raw data.
import boto3
from botocore.exceptions import NoCredentialsError

glue = boto3.client('glue', region_name='ap-northeast-1')

def create_and_run_glue_job():
    job_name = 'etl_script_to_S3_silver_layer'
    script_location = 's3://weather-influence-on-sales/scripts/weather_etl_job_script.py'  # S3 path to the ETL script
    role_arn = 'arn:aws:iam::098131747502:role/weather_data_GlueServiceRole'  # Replace with your IAM role ARN

    # Create the Glue Job
    job_config={
        'Role':role_arn,
        'Command':{
            'Name': 'glueetl',
            'ScriptLocation': script_location,
            'PythonVersion': '3'
        },
        'DefaultArguments':{
            '--job-language': 'python',
            '--continuous-log-loggroup': '/aws-glue/jobs/{job_name}'
        },
        'GlueVersion': '4.0',
        'NumberOfWorkers': 2, # Adjust based on your requirements
        'WorkerType': 'G.1X'  # Standard worker type
    }
    try:
        glue.create_job(Name=job_name, **job_config)
        print(f"Created new Glue Job: {job_name}")
    except glue.exceptions.AlreadyExistsException:
            # if the job already exists, update it
        glue.update_job(JobName=job_name, JobUpdate=job_config)
        print(f"Updated existing Glue Job: {job_name}")
    except Exception as e:
        print(f"Error: {e}")
        return
    

    # Run the Glue Job
    response = glue.start_job_run(JobName=job_name)
    print(f"Started Glue Job '{job_name}' with Run ID: {response['JobRunId']}")

if __name__ == "__main__":
    create_and_run_glue_job()