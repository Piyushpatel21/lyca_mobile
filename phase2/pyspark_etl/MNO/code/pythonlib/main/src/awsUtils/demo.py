import boto3


def getawsClient(service):
    """If user want to test in local use session in below format"""
    session = boto3.Session(aws_access_key_id='AKIAXBQ6FEWBGATUWVMM', aws_secret_access_key='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', region_name='eu-west-2')
    return session.client(service)

getawsClient("s3")