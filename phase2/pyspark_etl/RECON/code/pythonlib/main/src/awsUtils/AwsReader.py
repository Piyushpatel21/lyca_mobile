import boto3


class AwsReader:

    @staticmethod
    def getawsClient(service):
        """If user want to test in local use session in below format"""
        return boto3.client(service)

    @staticmethod
    def gets3FileContent(s3client, bucketName, key):
        return s3client.get_object(Bucket=bucketName, Key=key)

    @staticmethod
    def s3ReadFile(service, bucket_name, file):
        s3 = AwsReader.getawsClient(service)
        contentObj = AwsReader.gets3FileContent(s3, bucket_name, file)
        return contentObj['Body'].read().decode('utf-8')
