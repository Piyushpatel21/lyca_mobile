import boto3
import base64
import json
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, DoubleType, LongType, FloatType, DateType, TimestampType
from pyspark.conf import SparkConf
import pyspark
import sys
import argparse


sconf = pyspark.SparkConf()
sc = SparkContext()
glue_context = GlueContext(sc)
SparkSession = glue_context.spark_session
sconf.set("spark.sql.parquet.compression.codec", "snappy")
spark = SparkSession.builder.appName('RedshiftToS3').config(conf=sconf).getOrCreate()


def get_secret(secret_name, region_name=None):
    # Create a Secrets Manager client
    if region_name:
        client = boto3.client(
            'secretsmanager',
            region_name=region_name
        )
    else:
        client = boto3.client(
            'secretsmanager'
        )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return json.loads(secret)
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return decoded_binary_secret


redshiftsecret = 'Redshift/etl_user'
region = 'eu-west-2'
secrets = get_secret(secret_name=redshiftsecret, region_name=region)
print("The value for the Secrets")
print(secrets)
jdbcUsername = secrets["username"]
jdbcPassword = secrets["password"]
jdbcHostname = "uk-eu-west-2-311477489434-dev.c3rozf7wvu9s.eu-west-2.redshift.amazonaws.com"
jdbcPort = "5439"
jdbcDatabase = "fradev"
redshiftTmpDir = "s3://mis-dl-uk-eu-west-2-311477489434-dev-raw/tmp/redshift/rrbs/"
jdbcUrl = "jdbc:redshift://{jdbcHostname}:{jdbcPort}/{jdbcDatabase}?user={jdbcUsername}&password={jdbcPassword}" \
    .format(jdbcHostname=jdbcHostname, jdbcPort=jdbcPort, jdbcDatabase=jdbcDatabase,
            jdbcUsername=jdbcUsername,
            jdbcPassword=jdbcPassword)


def copyRedshiftToS3(tableName, column_name, column_value, db, partitionDateColumn, moduleName, countryName, subModule, bucket,start_date,end_date):
    query = """SELECT * FROM {schemaName}.{table} WHERE {columnName} between '{startDate}' AND '{endDate}'""".format(schemaName = db, table=tableName, columnName=column_name, startDate=start_date, endDate=end_date)
    redshiftTable = spark.read \
        .format("com.databricks.spark.redshift") \
        .option("url", jdbcUrl) \
        .option("query", query) \
        .option("forward_spark_s3_credentials", "true") \
        .option("tempdir", redshiftTmpDir) \
        .load()
    redshiftTable.count()
    #redshiftTable.select("msg_date_dt").show()
    redshiftTablePartionColumn = redshiftTable.withColumn(str(partitionDateColumn) + "-year", F.date_format(F.col(partitionDateColumn), "yyyy").cast(IntegerType())) \
        .withColumn(str(partitionDateColumn) + "-month", F.date_format(F.col(partitionDateColumn), "MM").cast(IntegerType())) \
        .withColumn(str(partitionDateColumn) + "-day", F.date_format(F.col(partitionDateColumn), "dd").cast(IntegerType()))
    redshiftTablePartionColumn.printSchema()

    redshiftTablePartionColumn.write.mode("overwrite").format("parquet").partitionBy(str(partitionDateColumn) + "-year", str(partitionDateColumn) + "-month", str(partitionDateColumn) + "-day").save("s3://" + str(bucket) + "/" + str(moduleName) + "/" + str(countryName) + "/" + str(subModule))


def parseArguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--tableName')
    parser.add_argument('--column_name')
    parser.add_argument('--column_value')
    parser.add_argument('--db')
    parser.add_argument('--partitionDateColumn')
    parser.add_argument('--moduleName')
    parser.add_argument('--countryName')
    parser.add_argument('--subModule')
    parser.add_argument('--bucket')
    parser.add_argument('--start_date')
    parser.add_argument('--end_date')
    known_arguments, unknown_arguments = parser.parse_known_args()
    arguments = vars(known_arguments)
    return arguments


if __name__ == '__main__':
    args = parseArguments()
    print("arguments********")
    print(args)
    copyRedshiftToS3(**args)
