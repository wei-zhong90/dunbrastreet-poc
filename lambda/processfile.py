import json
import boto3
from datetime import datetime
import urllib.parse


s3 = boto3.resource('s3')



def handler(event, context):
    print(event)
    
    date = datetime.today()
    year = date.strftime("%y")
    month = date.strftime("%m")
    day = date.strftime("%d")
    
    for record in event['Records']:
        bucket_name = record['s3']['bucket']['name']
        object_key = urllib.parse.unquote(record['s3']['object']['key'])
        
        file_name = object_key.split('/')[-1].split('.')[0]
        
        object = s3.Object(bucket_name,object_key)
        
        object.download_file(f'/tmp/{file_name}')
        
        with open(f'/tmp/{file_name}') as f:
            lines = f.readlines()
        

        data = []
        for i, line in enumerate(lines):
            data.append(json.loads(line))
            if i % 5000 == 0 and i != 0:
                str = json.dumps(data)

                binary = bytes(str, 'utf-8')
                new_object = s3.Object(bucket_name, f'largetxtfile/year={year}/month={month}/day={day}/index-{i}-{file_name}.json')
                print(binary)
                new_object.put(Body=binary)
                data = []
            else:
                continue
        str = json.dumps(data)
        binary = bytes(str, 'utf-8')
        new_object = s3.Object(bucket_name, f'largetxtfile/year={year}/month={month}/day={day}/index-{i}-{file_name}.json')
        new_object.put(Body=binary)
            
        
    return

# with open('data/EnterpriseBaseInfo-20211122_6.txt') as f:
#     lines = f.readlines()

# data = []

