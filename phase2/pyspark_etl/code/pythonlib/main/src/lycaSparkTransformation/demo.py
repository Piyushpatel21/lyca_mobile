########################################################################
# description     : Building StructType schema for data files          #
# author          : Naren K(narendra.kumar@cloudwick.com),             #
#                   Tejveer Singh(tejveer.singh@cloudwick.com)         #
#                   Shubhajit Saha(shubhajit.saha@cloudwick.com)       #
# contributor     :                                                    #
# version         : 1.0                                                #
# notes           :                                                    #
########################################################################

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType, TimestampType, LongType
import json
from pyspark.sql import SparkSession


class Demo:

    @staticmethod
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

    @staticmethod
    def json_parser(file):
        """:parameter filepath - Json file path
           :return streaming byte of input file"""
        with open(file) as f:
            return json.load(f)

    @staticmethod
    def structTypemapping(JsonPath) -> StructType:
        """:parameter JsonPath - schema file path
           :return StructType schema for a source file"""
        data = Demo.json_parser(JsonPath)
        fieldStruct = StructType([])
        for col in data:
            fieldStruct.add(
                StructField((col["column_name"]), Demo.dataType(col["column_type"]), col["required"]))
        return fieldStruct


sparkSession = SparkSession.builder.master("local").appName("appname").getOrCreate()
schemafile = "/Users/narenk/PycharmProjects/lycamobile-etl-movements/phase2/pyspark_etl/code/config/rrbs_src_fct_sms.json"
structtype = Demo.structTypemapping(schemafile)
print(structtype)
file = "/Users/narenk/PycharmProjects/lycamobile-etl-movements/phase2/pyspark_etl/code/pythonlib/test/resources/UKR6_CS_08_05_2020_05_36_50_24934.csv"
df = sparkSession.read.option("header", "false").option("dateFormat", 'dd-MM-yyyy').schema(structtype).csv(file)
df.show(20, False)
df.printSchema()
