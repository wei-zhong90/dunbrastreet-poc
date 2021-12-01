import json
import uuid
from datetime import datetime
import boto3
import os
import base64

client = boto3.client("s3")
bucketname = os.environ['BUCKET_NAME']

def handler(event, context):
  print("request:", json.dumps(event))
  
  now = datetime.now()
  date = datetime.today()
  year = date.strftime("%y")
  month = date.strftime("%m")
  day = date.strftime("%d")
  
  payload = event['body']
  
  client.put_object(
    Body=payload,
    Bucket=bucketname,
    Key=f'raw/year={year}/month={month}/day={day}/{uuid.uuid4()}.json'
  )
  
  response = {
    'statusCode': 200,
    'headers': { "Content-Type": "text/plain" },
    'body': f"json info is stored in {bucketname} by POST request at {event['path']}\n"
  }
  return response