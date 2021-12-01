import json
# import uuid
from datetime import datetime
import boto3
import os
# import base64

transfer = boto3.client('transfer')
s3 = boto3.resource('s3')


def handler(event, context):
    print(json.dumps(event))
    
    now = datetime.now()
    date = datetime.today()
    year = date.strftime("%y")
    month = date.strftime("%m")
    day = date.strftime("%d")
    
    bucket_name = event['fileLocation']['bucket']
    object_key = event['fileLocation']['key']
    
    file_name = object_key.split('/')[-1]
    
    copy_source = {
        'Bucket': bucket_name,
        'Key': object_key
    }
    bucket = s3.Bucket(bucket_name)
    original_object = bucket.Object(object_key)
    obj = bucket.Object(f'raw/year={year}/month={month}/day={day}/{file_name}')
    obj.copy(copy_source)
    original_object.delete()
    
    

    # call the SendWorkflowStepState API to notify worfklows for an update on the step with a SUCCESS or a FAILURE
    response = transfer.send_workflow_step_state(
        WorkflowId=event['serviceMetadata']['executionDetails']['workflowId'],
        ExecutionId=event['serviceMetadata']['executionDetails']['executionId'],
        Token=event['token'],
        Status='SUCCESS'
    )

    print(json.dumps(response))

    return {
        'statusCode': 200,
        'body': json.dumps(response)
    }
