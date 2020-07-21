import boto3


class AwsReader:

    @staticmethod
    def get_aws_client(service):
        """If user want to test in local use session in below format"""
        return boto3.client(service)

    @staticmethod
    def get_s3_file_content(s3client, bucketName, key):
        return s3client.get_object(Bucket=bucketName, Key=key)

    @staticmethod
    def s3_read_file(service, bucket_name, file):
        s3 = AwsReader.get_aws_client(service)
        contentObj = AwsReader.get_s3_file_content(s3, bucket_name, file)
        return contentObj['Body'].read().decode('utf-8')
