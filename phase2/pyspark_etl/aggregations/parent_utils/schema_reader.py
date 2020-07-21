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
from parent_utils.json_processor import JsonProcessor


class SchemaReader:

    @staticmethod
    def data_type(column_type):
        """:parameter columnType, which we have in schema file for each column
           :return actual datatype according to spark compatibility"""
        if column_type in ("smallint", "integer"):
            return IntegerType()
        elif column_type in "bigint":
            return LongType()
        elif column_type in "numeric":
            return DecimalType(22, 6)
        elif column_type in "character":
            return StringType()
        elif column_type in "date":
            return DateType()
        elif column_type in ("timestamp without time zone", "timestamp"):
            return TimestampType()
        else:
            return StringType()

    @staticmethod
    def struct_typemapping(json_path) -> StructType:
        """:parameter JsonPath - schema file path
           :return StructType schema for a source file"""
        data = JsonProcessor.json_parser(json_path)
        fieldStruct = StructType([])
        for col in data:
            fieldStruct.add(
                StructField(col["column_name"], SchemaReader.dataType(col["column_type"]), col["required"]))
        return fieldStruct