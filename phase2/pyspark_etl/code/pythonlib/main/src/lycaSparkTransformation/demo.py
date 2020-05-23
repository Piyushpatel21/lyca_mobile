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


udf_calc = udf(chekNull, StringType())
sparkSession = SparkSession.builder.master("local").appName("appname").getOrCreate()
# schemafile = "/Users/narenk/PycharmProjects/lycamobile-etl-movements/phase2/pyspark_etl/code/config/rrbs_src_fct_sms.json"
# structtype = structTypemapping(schemafile)
# print(structtype)
# file = "/Users/narenk/PycharmProjects/lycamobile-etl-movements/phase2/pyspark_etl/code/pythonlib/test/resources/UKR6_CS_08_05_2020_05_36_50_24934.csv"
# df = sparkSession.read.option("header", "false").option("dateFormat", 'dd-MM-yyyy').csv(file)
# df.show(20, False)
# fields = df.schema.fields
# stringFields = filter(lambda f: isinstance(f.dataType, StringType), fields)
# stringFieldsTransformed = map(lambda f: udf_calc(fa.col(f.name)), stringFields)
# nonStringFields = map(lambda f: fa.col(f.name), filter(lambda f: not isinstance(f.dataType, StringType), fields))
# allFields = [*stringFieldsTransformed, *nonStringFields]
# val = df.select(allFields).show(20, False)

# df = sparkSession.createDataFrame(['F'], ['batch_status'])
# df.show(20, False)

# myFloatRdd = ['F']
# myFloatRdd.map(lambda x: (x, )).toDF()
