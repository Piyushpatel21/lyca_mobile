#!/usr/bin/env python3

import json
import boto3
import sys
import time
from datetime import datetime
start_time = time.clock()
now = datetime.now()
bucket = sys.argv[1]
prefix = sys.argv[2]
domain = prefix.split("/")[1]
if len(prefix.split("/")) == 5:
    subdomain = prefix.split("/")[3]
    country = prefix.split("/")[2]
    filename = 'log_' + domain + '_' +country +'_'+ subdomain + '_' + now.strftime("%d_%m_%Y_%H_%M_%S") +'.json'
else:
    filename = 'log_' + domain + '_' + now.strftime("%d_%m_%Y_%H_%M_%S") +'.json'
ProfileName = sys.argv[3]
session = boto3.Session(profile_name = ProfileName)
#session = boto3.Session(profile_name='mis_lycamobile_dev')
s3_c = session.client('s3')
s3_r = session.resource('s3')
paginator = s3_c.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
for page in pages:
        for obj in page['Contents']:
            if obj['Key'][-1] != '/':
                with open(filename, 'a') as outfile:
                    outfile.write(json.dumps(obj,default=str))
                    outfile.write("\n")
                      # print(json.dumps(obj,default=str))
#s3_r.Bucket(bucket).upload_file(filename,'transfer_log/'+filename)
print(time.clock() - start_time)
