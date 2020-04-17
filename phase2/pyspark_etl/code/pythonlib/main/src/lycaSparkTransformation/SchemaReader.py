from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType
from commonUtils.CommandLineProcessor import CommandLineProcessor


class SchemaReader:

    @staticmethod
    def dataType(columnType):
        if columnType in ("smallint"):
            return StringType()
        elif columnType in ("character", "timestamp without time zone"):
            return StringType()
        elif columnType in "date":
            return StringType()
        else:
            return StringType()

    @staticmethod
    def structTypemapping(JsonPath) -> StructType:
        try:
            data = CommandLineProcessor.json_parser(JsonPath)
            fieldStruct = StructType([])
            for col in data:
                fieldStruct.add(
                    StructField(col["column_name"], SchemaReader.dataType(col["column_type"]), col["required"]))
            return fieldStruct
        except ValueError:
            print('Decoding JSON has failed')