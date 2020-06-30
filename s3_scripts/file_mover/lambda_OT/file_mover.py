#!/usr/bin/env python

"""
Here we use the DynamoDB, and S3 boto3 clients to move the file according to a
mapping table and date stored in the dynamodb table
We expect SNS events from S3
"""

import json
import logging
import re
import os
import urllib
from datetime import datetime
import boto3
from boto3.dynamodb.conditions import Key, Attr

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

###
# Getting our environment variables
###
# Our DynamoDB table
mapping_table = os.environ['MAPPING_TABLE']
transferLog_table = os.environ['TRANSFERLOG_TABLE']

### boto3 resources
dynamodb_client = boto3.client('dynamodb')
dynamodb_resource = boto3.resource('dynamodb')
table = dynamodb_resource.Table(transferLog_table)
s3 = boto3.resource('s3')

def year_month_day(objectname):
    """
    Function to extract the Year Month and date from filename
    """
    pattern = '([0-9]{2}_[0-9]{2}_[0-9]{4}_|_[0-9]{8}|[0-9]{4}_[0-9]{2}_[0-9]{2}_|-[0-9]{8}|[0-9]{2}-[0-9]{2}-[0-9]{4}.|[a-zA-Z]_[0-9]{2}_[0-9]{2}_[0-9]{4}.)'#|[a-zA-Z]_[0-9]{8}.)'
    pattern1 = '[0-9]{2}_[0-9]{2}_[0-9]{4}_'
    pattern2 = '_[0-9]{8}'
    pattern3 = '[0-9]{4}_[0-9]{2}_[0-9]{2}_'
    pattern4 = '-[0-9]{8}'
    rec_ptrn1 = '[0-9]{2}-[0-9]{2}-[0-9]{4}.'
   # rec_ptrn2 = '[a-zA-Z]_[0-9]{8}.'
    rec_ptrn3 = '[a-zA-Z]_[0-9]{2}_[0-9]{2}_[0-9]{4}.'
    result = re.findall(pattern, objectname) 
    dd = ''
    mm = ''
    yyyy = ''
    ymd = ''
    flag = False
    if result:
        if re.match(pattern1,result[0]):
            dd = result[0].split('_')[0]
            mm = result[0].split('_')[1]
            yyyy = result[0].split('_')[2]
            flag = True
        elif re.match(pattern3,result[0]):
            dd = result[0].split('_',3)[2]
            mm = result[0].split('_',3)[1]
            yyyy = result[0].split('_',3)[0]
            flag = True
        elif re.match(pattern2,result[0]):
            yyyy = result[0].split('_')[1][0:4]
            mm = result[0].split('_')[1][4:6]
            dd = result[0].split('_')[1][6:8]
            flag = True
        elif re.match(pattern4,result[0]):
            yyyy = result[0].split('-')[1][0:4]
            mm = result[0].split('-')[1][4:6]
            dd = result[0].split('-')[1][6:8]
            flag = True
        elif re.match(rec_ptrn1,result[0]):  #[0-9]{2}-[0-9]{2}-[0-9]{4}.
            ptrn = result[0].split('.',1)[0]
            yyyy = ptrn.split('-')[2]
            mm = ptrn.split('-')[1]
            dd = ptrn.split('-')[0]
            flag = True
   #     elif re.match(rec_ptrn2,result[0]): #_[0-9]{8}.
    #        print("rec_ptrn2")
     #       ptrn = result[0].split('.',1)[0]
      #      yyyy = ptrn.split('_')[1][0:4]
       #     mm = ptrn.split('_')[1][4:6]
        #    dd = ptrn.split('_')[1][6:8]
         #   flag = True
        elif re.match(rec_ptrn3,result[0]): #_[0-9]{2}_[0-9]{2}_[0-9]{4}.
            ptrn = result[0].split('.',1)[0]
            yyyy = ptrn.split('_')[3]
            mm = ptrn.split('_')[2]
            dd = ptrn.split('_')[1]
            flag = True
    else:
        ymd = "pattern not found"
    if flag:
        if ((yyyy >= '2000' and yyyy <= '2100') and (mm >= '01' and mm <= '12') and (dd >= '01' and dd <= '31')):
            ymd = yyyy + "/" + mm + "/" + dd
        else:
            ymd = "incorrect date"
    return (ymd)

def dynamodb_lookup(key):
    """
    Return the target mapping Dir
    """
    LOGGER.info('Looking up {} in {}'.format(key, mapping_table))
    response = dynamodb_client.get_item(Key={'SourceDir': {'S': key}}, TableName=mapping_table)
    outputpath = response['Item']['OutputDir']['S']
    return outputpath


def insert_transferlog(key, Etag, LastModified, size, targetSystem, targetFile):
    """
    Enter file relocation logs to dynamoDB table
    """
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    response = table.put_item(
        Item={
            'FilenameOrig': key.split("/")[-1],
            'SourceSystem': "/".join(key.split("/")[:-1]),
            'FilenameTgt': targetFile,
            'TargetSystem': targetSystem,
            'FileLandingDate': LastModified,
            'FileRegDate': dt_string,
            'FileETag': Etag,
            'Size': size,
            'Status': 'Complete',  # need to be updated in later version
            'Version': 0,  # need to be updated in later version
        }
    )
    LOGGER.info("DynamoDB entry made for {}".format(key))


def map_to_output(key):
    key = "/".join(key.split("/")[:-1])
    outputpath = dynamodb_lookup(key)
    return outputpath


def lambda_handler(event, context):
    """
    Main Lambda handler
    """
    # our incoming event is the S3 put event notification
    s3_message = json.loads(event['Records'][0]['Sns']['Message'])
    # get the object key and bucket name
    print(s3_message)
    key = s3_message['Records'][0]['s3']['object']['key']  # landing/RRBS/UK/CDR_2019_03_01_blahblah.csv
    print(key)
    key = urllib.parse.unquote(key)
    bucket = s3_message['Records'][0]['s3']['bucket']['name']
    source_object = {
        'Bucket': bucket,
        'Key': key
    }
    LOGGER.info("Processing {}".format(key))
    Etag = s3_message['Records'][0]['s3']['object']['eTag']
    LastModified = s3_message['Records'][0]['eventTime']
    size = s3_message['Records'][0]['s3']['object']['size']

    objectname = key.split("/")[-1]
    sourceSystem = "/".join(key.split("/")[:-1])
    path = map_to_output(key)
    if key[-1] != "/":
        if (sourceSystem == 'landing/MNO/GBR/202004' OR sourceSystem == 'landing/MNO/FRA/202004'):
            ymd = year_month_day(objectname)
            if objectname.find('GPRS') != -1:
                new_filename = path + '/GPRS/' + ymd + '/' + objectname
            elif objectname.find('SMS') != -1:
                new_filename = path + '/SMS/' + ymd + '/' + objectname
            elif objectname.find('TOPUP') != -1:
                new_filename = path + '/TOPUP/' + ymd + '/' + objectname
            else:
                new_filename = path + '/VOICE/' + ymd + '/' + objectname
        else:
            #targetFile = objectname
            new_filename = path + '/' + objectname
        targetSystem = "/".join(new_filename.split("/")[:-1])
        s3.meta.client.copy(source_object, bucket, new_filename, ExtraArgs={'ACL': 'bucket-owner-full-control'})
        insert_transferlog(key, Etag, LastModified, size, targetSystem, objectname)  # insert the logs to dynamodb table
        #s3.meta.client.delete_object(Bucket=bucket, Key=key)
    LOGGER.info("Finished")
