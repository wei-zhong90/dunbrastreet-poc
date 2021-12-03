import json
import boto3
from datetime import datetime
import urllib.parse
import pandas as pd
import copy
import uuid


s3 = boto3.resource('s3')

s3_client = boto3.client('s3')

def remove_nest(dictionary):
    keys = []
    for key, content in dictionary.items():
        if type(content) is type([]):
            keys.append(key)
    for k in keys:
        dictionary.pop(k)


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
        
        sample_object = json.loads(lines[0])
        header_ori = copy.deepcopy(sample_object)
        remove_nest(header_ori)
        headers = []
        
        for key, value in header_ori.items():
            headers.append(key)
        
        headers.append('ENT_SHAREHOLDER')
        
        nature_persons = set()
        data = {}
        
        for h in headers:
            data[h] = []
        
        for line in lines:
            payload = json.loads(line)
            line_header = copy.deepcopy(headers)
            for h, v in payload.items():
                if h == 'LEGALPERSON':
                    nature_persons.add(v)
                if h in headers:
                    data[h].append(v)
                    line_header.pop(line_header.index(h))
            if 'SHAREHOLDER' in payload:
                shareholders = []
                for content in payload['SHAREHOLDER']:
                    if 'inv' in content:
                        nature_persons.add(content['inv'])
                        shareholders.append(content['inv'])
                    else:
                        break
                if len(shareholders) != 0:
                    # print(shareholders)
                    data['ENT_SHAREHOLDER'].append(shareholders)
                    line_header.pop(line_header.index('ENT_SHAREHOLDER'))
            for h in line_header:
                data[h].append('')
            
        
        df = pd.DataFrame(data=data)
        
        should_be_removed = set()
        for p in nature_persons:
            if len(p) > 4:
                should_be_removed.add(p)
        
        for i in should_be_removed:
            nature_persons.remove(i)
        nature_persons.remove('')
        
        vertice_df = df.loc[:, ~df.columns.isin(['ENT_SHAREHOLDER', 'LEGALPERSON'])]
        vertice_df.rename(columns={'ENTNAME': '~id'}, inplace=True)
        vertice_df['~label'] = 'company'
        vertice_df['~label']

        person_headers = list(vertice_df.columns.values)
        person_data = {}
        
        for h in person_headers:
            person_data[h] = []
        person_headers.pop(person_headers.index('~id'))
        person_headers.pop(person_headers.index('~label'))
        
        for p in nature_persons:
            person_data['~id'].append(p)
            person_data['~label'].append('nature_person')
            for other_headers in person_headers:
                person_data[other_headers].append('')
        
        persons_df = pd.DataFrame(data=person_data)
        vertice_df = pd.concat([vertice_df, persons_df], ignore_index = True, axis = 0)
        
        edge_data = {
            '~from': [],
            '~to': [],
            '~label': []
        }
        
        possible_ent_names = set(vertice_df['~id'])
        
        for index, row in df.iterrows():
            edge_data['~from'].append(row['LEGALPERSON'])
            edge_data['~to'].append(row['ENTNAME'])
            edge_data['~label'].append('legal_person')
            for p in row['ENT_SHAREHOLDER']:
                if p in possible_ent_names:
                    edge_data['~from'].append(p)
                    edge_data['~to'].append(row['ENTNAME'])
                    edge_data['~label'].append('shareholder')
        
        existing_names = set(edge_data['~from'])
        
        missing_names = possible_ent_names.difference(existing_names)
        
        edge_df = pd.DataFrame(data=edge_data)
        edge_df.insert(1, '~id', range(1, 1+ len(edge_df)))
        
        vertice_filename = f'v-{uuid.uuid4()}.csv'
        edge_filename = f'e-{uuid.uuid4()}.csv'
        
        vertice_df.to_csv(f'/tmp/{vertice_filename}', index=False)
        edge_df.to_csv(f'/tmp/{edge_filename}', index=False)
        
        
        
        s3_client.upload_file(f'/tmp/{vertice_filename}', bucket_name, f'processed-data/year={year}/month={month}/day={day}/vertice/{file_name}.csv')
        s3_client.upload_file(f'/tmp/{edge_filename}', bucket_name, f'processed-data/year={year}/month={month}/day={day}/edge/{file_name}.csv')
        
        print(missing_names)
        
        return
    
        
    #     vertice_object = s3.Object(bucket_name, f'processed-data/year={year}/month={month}/day={day}/vertice/{file_name}.csv')
    #     edge_object = s3.Object(bucket_name, f'processed-data/year={year}/month={month}/day={day}/edge/{file_name}.csv')
    #     data = []
    #     for i, line in enumerate(lines):
    #         data.append(json.loads(line))
    #         if i % 5000 == 0 and i != 0:
    #             str = json.dumps(data)

    #             binary = bytes(str, 'utf-8')
    #             new_object = s3.Object(bucket_name, f'largetxtfile/year={year}/month={month}/day={day}/index-{i}-{file_name}.json')
    #             print(binary)
    #             new_object.put(Body=binary)
    #             data = []
    #         else:
    #             continue
    #     str = json.dumps(data)
    #     binary = bytes(str, 'utf-8')
    #     new_object = s3.Object(bucket_name, f'largetxtfile/year={year}/month={month}/day={day}/index-{i}-{file_name}.json')
    #     new_object.put(Body=binary)
            
        
    # return

# with open('data/EnterpriseBaseInfo-20211122_6.txt') as f:
#     lines = f.readlines()

# data = []

