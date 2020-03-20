#!/usr/bin/env python

"""
Here we use the DynamoDB, and S3 boto3 clients to move the file according to a 
mapping table and date stored in the dynamodb table
We expect SNS events from S3
"""

import boto3
import json
import re
import os
import urllib

###
# Getting our environment variables
###
# Our DynamoDB table
mapping_table = os.environ['MAPPING_TABLE']

### boto3 resources
dynamodb_client=boto3.client('dynamodb')
s3=boto3.resource('s3')

def year_month_day(objectname):
   # x = string.objectname
    pattern = '([0-9]{2}_[0-9]{2}_[0-9]{4}_|[0-9]{8}_|[0-9]{4}_[0-9]{2}_[0-9]{2}_)'
    pattern1 = '[0-9]{2}_[0-9]{2}_[0-9]{4}_'
    pattern2 = '[0-9]{8}_'
    pattern3 = '[0-9]{4}_[0-9]{2}_[0-9]{2}_'
    result = re.findall(pattern, objectname) 
    dd = ''
    mm = ''
    yyyy = ''
    flag = False
    if result:
        if re.match(pattern1,result[0]):
            dd = result[0].split('_',3)[0]
            mm = result[0].split('_',3)[1]
            yyyy = result[0].split('_',3)[2]
            flag = True

        elif re.match(pattern3,result[0]):
            dd = result[0].split('_',3)[2]
            mm = result[0].split('_',3)[1]
            yyyy = result[0].split('_',3)[0]
            flag = True

        elif re.match(pattern2,result[0]):
            yyyy = result[0].split('_',1)[0][0:4]
            mm = result[0].split('_',1)[0][4:6]
            dd = result[0].split('_',1)[0][6:8]
            flag = True
    else:
        return "pattern not found"
    if flag:
        if ((yyyy >= '2000' and yyyy <= '2100') and (mm >= '01' and mm <='12') and (dd >= '01' and dd <='31')):
            ymd = yyyy + "/" + mm + "/" + dd
            return (ymd)
        else :
            return "incorrect date"


def dynamodb_lookup(key):
    print(mapping_table)
    print(key)
    response = dynamodb_client.get_item(Key = { 'SourceDir' : {'S': key}}, TableName=mapping_table)
    outputpath = response['Item']['OutputDir']['S']
    return outputpath
    
def map_to_output(key):
    key = "/".join(key.split("/")[:-1])
    outputpath = dynamodb_lookup(key)
    return outputpath
    
def lambda_handler(event, context):
    """
    Main Lambda handler
    """
    print(json.dumps(event))
    # our incoming event is the S3 put event notification
    s3_message = json.loads(event['Records'][0]['Sns']['Message'])
    # get the object key and bucket name
    key = s3_message['Records'][0]['s3']['object']['key'] # landing/RRBS/UK/CDR_2019_03_01_blahblah.csv
    key = urllib.parse.unquote(key)
    bucket = s3_message['Records'][0]['s3']['bucket']['name']
    source_object = {
        'Bucket' : bucket,
        'Key' : key
        }
    prefix = "/".join(key.split("/")[:-1])
    #check for key value, key should contain filename
    if key[-1] != "/":
        objectname = key.split("/")[-1]
        ymd = year_month_day(objectname)
        new_filename = map_to_output(key) + '/' + ymd + '/' + objectname
    
    s3.meta.client.copy(source_object, bucket, new_filename,ExtraArgs={'ACL': 'bucket-owner-full-control'})
    s3.meta.client.delete_object(Bucket = bucket, Key = key)
