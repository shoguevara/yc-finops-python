import pandas as pd
import json
import csv
import os
import boto3
import sys
from io import StringIO
import requests

def getbillingcsv(bucket_name,object_key):
    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net'
    )
    csv_obj = s3.get_object(Bucket=bucket_name, Key=object_key)
    body = csv_obj['Body']
    csv_string = body.read().decode('utf-8')
    df1 = pd.read_csv(StringIO(csv_string))
    return df1

def getservicejson(serviceurl,folderid,iamtoken):
    response = requests.get("%s%s" % (serviceurl,folderid), headers={'Authorization': 'Bearer %s'  % iamtoken})
    return response

#def transform(inputcsv,inputjson):
#   with open('instaces.json', encoding='utf-8') as f:
#       data = json.loads(f.read())
#   jsonBody = pd.json_normalize(data).rename(columns={'id': 'resource_id'})
#   labels_columns = [col for col in jsonBody.columns if "labels." in col]
#   labels_columns.append("resource_id")
#   jsonBody_subset = jsonBody[labels_columns]
#   df1 = pd.read_csv('20230213.csv')
#   merged_data = df1.merge(jsonBody_subset,on=["resource_id"],how="left")
#   merged_data.to_csv('merged.csv')   
#   return merged_data

#def saveresultingcsv(bucket_name,object_key):
#    session = boto3.session.Session()
#    s3 = session.client(
#        service_name='s3',
#        endpoint_url='https://storage.yandexcloud.net'
#    )
#    transformed_bucket = bucket_name+'_transformed'
#    bucket = s3.create_bucket(Bucket=transformed_bucket)

def handler(event, context):
    bucket_name = event['messages'][0]['details']['bucket_id']
    object_key = event['messages'][0]['details']['object_id']
    serviceurl = "https://compute.api.cloud.yandex.net/compute/v1/instances?folderId="
    folder = event['messages'][0]['event_metadata']['folder_id']
    iamtoken = context.token['access_token']
    print(getservicejson(serviceurl,folder,iamtoken).json())
    return {
        'statusCode': 200,
        'body': 'Success!',
    }
