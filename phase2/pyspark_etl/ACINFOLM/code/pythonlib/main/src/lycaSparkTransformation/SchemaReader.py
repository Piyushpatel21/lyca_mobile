########################################################################
# description     : Building StructType schema for data files          #
# author          : Naren K(narendra.kumar@cloudwick.com),             #
#                   Tejveer Singh(tejveer.singh@cloudwick.com)         #
#                   Shubhajit Saha(shubhajit.saha@cloudwick.com)       #
# contributor     :                                                    #
# version         : 1.0                                                #
# notes           :                                                    #
########################################################################

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType, LongType, TimestampType
from commonUtils.JsonProcessor import JsonProcessor


class SchemaReader:

    @staticmethod
    def dataType(columnType):
        """:parameter columnType, which we have in schema file for each column
           :return actual datatype according to spark compatibility"""
        if columnType in ("smallint", "integer"):
            return IntegerType()
        elif columnType in "bigint":
            return LongType()
        elif columnType in "numeric":
            return DecimalType(22, 6)
        elif columnType in "character":
            return StringType()
        elif columnType in "date":
            return DateType()
        elif columnType in ("timestamp without time zone", "timestamp"):
            return TimestampType()
        else:
            return StringType()

    @staticmethod
    def structTypemapping(JsonPath) -> StructType:
        """:parameter JsonPath - schema file path
           :return StructType schema for a source file"""
        data = JsonProcessor.json_parser(JsonPath)
        fieldStruct = StructType([])
        for col in data:
            fieldStruct.add(
                StructField(col["column_name"], SchemaReader.dataType(col["column_type"]), col["required"]))
        return fieldStruct