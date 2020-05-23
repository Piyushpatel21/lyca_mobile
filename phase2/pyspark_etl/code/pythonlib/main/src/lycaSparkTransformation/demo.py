########################################################################
# description     : Building StructType schema for data files          #
# author          : Naren K(narendra.kumar@cloudwick.com),             #
#                   Tejveer Singh(tejveer.singh@cloudwick.com)         #
#                   Shubhajit Saha(shubhajit.saha@cloudwick.com)       #
# contributor     :                                                    #
# version         : 1.0                                                #
# notes           :                                                    #
########################################################################
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType, TimestampType, \
    LongType
import json
from pyspark.sql import functions as fa
from pyspark.sql import SparkSession


def dataType(columnType):
    """:parameter columnType, which we have in schema file for each column
       :return actual datatype according to spark compatibility"""
    if columnType in ("smallint", "integer"):
        return StringType()
    elif columnType in "bigint":
        return StringType()
    elif columnType in "numeric":
        return StringType()
    elif columnType in "character":
        return StringType()
    elif columnType in "date":
        return StringType()
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


# udf_calc = udf(chekNull, StringType())
sparkSession = SparkSession.builder.master("local").config("spark.sql.crossJoin.enabled", "true").appName("appname").getOrCreate()
# schemafile = "/Users/narenk/PycharmProjects/lycamobile-etl-movements/phase2/pyspark_etl/code/config/rrbs_src_fct_sms.json"
# structtype = structTypemapping(schemafile)
# print(structtype)
file = "/Users/narenk/PycharmProjects/lycamobile-etl-movements/phase2/pyspark_etl/code/pythonlib/test/resources/sample.csv"
df1 = sparkSession.read.option("header", "false").option("dateFormat", 'dd-MM-yyyy').csv(file)
df2 = df1.withColumn("filename", fa.lit('sample'))
df3 = df2.groupBy('filename').agg(fa.count('_c1').alias('lateCDR'))

file = "/Users/narenk/PycharmProjects/lycamobile-etl-movements/phase2/pyspark_etl/code/pythonlib/test/resources/sample2.csv"
df4 = sparkSession.read.option("header", "false").option("dateFormat", 'dd-MM-yyyy').csv(file)
df5 = df4.withColumn("filename", fa.lit('sample'))
df6 = df5.groupBy('filename').agg(fa.count('_c1').alias('lateCDR1'))

df3.show(20, False)
df6.show(20, False)
output = df3.join(df6, df3['filename'] == df6['filename']).select(df3['filename'], df3['lateCDR'], df6['lateCDR1'])
output.show(20, False)


