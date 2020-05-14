import boto3
import json


bucket = 'python-sdk-sample-3d568b7c-b4d8-444e-88c8-fb7f455727d4'
key = 'app_module_level_properties.json'


def gets3Client(service):
    session = boto3.Session(profile_name="aws_naren")
    return session.client(service)


def gets3FileContent(s3client, bucketName, key):
    return s3client.get_object(Bucket=bucketName, Key=key)


s3 = gets3Client('s3')
contentObj = gets3FileContent(s3, bucket, key)
data = contentObj['Body'].read().decode('utf-8')


def json_parser(filepath):
    """:parameter filepath - Json file path
       :return streaming byte of input file"""
    return json.loads(filepath)


d = json_parser(data)
print(d)
