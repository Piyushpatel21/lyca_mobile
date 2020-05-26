########################################################################
# description     : Building StructType schema for data files          #
# author          : Naren K(narendra.kumar@cloudwick.com),             #
#                   Tejveer Singh(tejveer.singh@cloudwick.com)         #
#                   Shubhajit Saha(shubhajit.saha@cloudwick.com)       #
# contributor     :                                                    #
# version         : 1.0                                                #
# notes           :                                                    #
########################################################################
from datetime import datetime, timedelta

from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType, TimestampType, \
    LongType
import json
from pyspark.sql import functions as py_function
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


def dataType(columnType):
    """:parameter columnType, which we have in schema file for each column
       :return actual datatype according to spark compatibility"""
    if columnType in ("smallint", "integer"):
        return IntegerType()
    elif columnType in "bigint":
        return LongType()
    elif columnType in "numeric":
        return DecimalType()
    elif columnType in "character":
        return StringType()
    elif columnType in "date":
        return DateType()
    else:
        return StringType()


def json_parser(file):
    """:parameter filepath - Json file path
       :return streaming byte of input file"""
    with open(file) as f:
        return json.load(f)


def structTypemapping(JsonPath) -> StructType:
    """:parameter JsonPath - schema file path
       :return StructType schema for a source file"""
    data = json_parser(JsonPath)
    fieldStruct = StructType([])
    for col in data:
        fieldStruct.add(
            StructField((col["column_name"]), dataType(col["column_type"]), col["required"]))
    return fieldStruct


def chekNull(x):
    if x != '':
        return 'AA'


def getstatus(sparkSession: SparkSession, batch_ID: int, column, status) -> DataFrame:
    schema = StructType([StructField('batch_ID', IntegerType(), True), StructField(column, StringType(), True)])
    data = [(batch_ID, status)]
    rdd = sparkSession.sparkContext.parallelize(data)
    return sparkSession.createDataFrame(rdd, schema)


# udf_calc = udf(chekNull, StringType())
sparkSession = SparkSession.builder.master("local").appName("appname").getOrCreate()
# schemafile = "/Users/narenk/PycharmProjects/lycamobile-etl-movements/phase2/pyspark_etl/code/config/rrbs_src_fct_sms.json"
# structtype = structTypemapping(schemafile)
# print(structtype)
# file = "/Users/narenk/PycharmProjects/lycamobile-etl-movements/phase2/pyspark_etl/code/pythonlib/test/resources/UKR6_CS_08_05_2020_04_15_58_24910.cdr"
# BATCH_START_DT = datetime.now()
# df_source = sparkSession.read.option("header", "false").schema(structtype).csv(file).withColumn('batch_id', py_function.lit(101)).withColumn('filename', py_function.lit('sample.csv'))
# df_source.show(20, False)
# df_source.printSchema()