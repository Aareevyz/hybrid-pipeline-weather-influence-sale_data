import boto3
import json

sqs = boto3.client('sqs')
QUEUE_URL = "https://sqs.ap-northeast-1.amazonaws.com/098131747502/weather_sales_export_queue.fifo"

def lambda_handler(event, context):
    # data for send to local 
    message_body = {
        "status": "READY",
        "layer":"GOLD",
        "date_key": event.get("date_key")
    }

    response = sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps(message_body),
        MessageGroupId="weather_sales_export_group",
        MessageDeduplicationId=context.aws_request_id
    )
    return {
        'statusCode': 200,
        'body': json.dumps('Message sent to SQS')
    }
