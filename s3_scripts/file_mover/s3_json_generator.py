import json
import boto3
import time
import sys

ProfileName = sys.argv[3]
bucket = sys.argv[4]
arn_bucket = 'arn:aws:s3:::'+bucket
session = boto3.Session(profile_name = ProfileName)
client = session.client('sns',region_name = 'eu-west-2')
def send_to_sns(topic_arn,message):
    #print(json.dumps(message))
    response = client.publish(TargetArn = topic_arn, Message = message)


def list_to_s3_event(listing,topic_arn):

    #do json load test listing
    #screate s3 message obj
    #bucket info in s3 event
    #return string to json dumps of s3 event
    jsonstring = '''{
      "Records": [
            {
        "eventVersion": "2.0",
        "eventSource": "aws:s3",
        "awsRegion": "eu-west-2",
        "eventTime": "1970-01-01T00:00:00.000Z",
        "eventName": "ObjectCreated:Put",
        "s3": {
            "s3SchemaVersion": "1.0",
            "bucket": {
                "name": "example-bucket",
                "arn": "arn:aws:s3:::example-bucket"
                },
            "object": {
                "key": "test/key",
                "size": 1024,
                "eTag": "0123456789abcdef0123456789abcdef"
                    }
                }
            }
        ]
      }'''
    jsonobject = json.loads(jsonstring)
    data = [json.loads(line) for line in open(listing, 'r')]
    i =0 
    for i in range(0,len(data)):
        jsonobject['Records'][0]['eventTime'] = data[i]['LastModified']
        jsonobject['Records'][0]['s3']['bucket']['name'] = bucket
        jsonobject['Records'][0]['s3']['bucket']['arn'] = arn_bucket
        jsonobject['Records'][0]['s3']['object']['key'] = data[i]['Key']
        jsonobject['Records'][0]['s3']['object']['size'] = data[i]['Size']
        jsonobject['Records'][0]['s3']['object']['eTag'] = data[i]['ETag'].strip('"')
        #print(json.dumps(jsonobject))  
        send_to_sns(topic_arn,json.dumps(jsonobject))
       # time.sleep(0.05)

print('Call function')
if __name__ == " __main__":
    print('main')
else:
    listing = sys.argv[1]
    topic_arn = sys.argv[2]
    print(listing,topic_arn)
    list_to_s3_event(listing,topic_arn)
    #load filename for each file in filename
    #s3_event = list_to_s3_event
    #send_to_sns(topic_arn,s3_eve:nt)

