import boto3


class AwsReader:

    @staticmethod
    def getawsClient(service):
        session = boto3.Session(aws_access_key_id='AKIAXBQ6FEWBGATUWVMM',     aws_secret_access_key='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', region_name='eu-west-2')
        return session.client(service)

    @staticmethod
    def gets3FileContent(s3client, bucketName, key):
        return s3client.get_object(Bucket=bucketName, Key=key)

    @staticmethod
    def s3ReadFile(service, bucket_name, file):
        s3 = AwsReader.getawsClient(service)
        contentObj = AwsReader.gets3FileContent(s3, bucket_name, file)
        return contentObj['Body'].read().decode('utf-8')
