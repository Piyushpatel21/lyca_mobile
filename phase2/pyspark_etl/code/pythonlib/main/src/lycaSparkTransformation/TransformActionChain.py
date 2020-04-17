
from lycaSparkTransformation.DataTranformation import DataTranformation
from lycaSparkTransformation.SchemaReader import SchemaReader
from lycaSparkTransformation.SparkSessionBuilder import SparkSessionBuilder
import os
from pyspark.sql import functions as py_function
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType


class TransformActionChain:
    def __init__(self, module, subModule, ap, sourceFilePath, schemaPath, files, inputdateColumn, outputdateColumn, mnthOrdaily, noOfdaysOrMonth):
        self.module = module
        self.subModule = subModule
        self.appname = ap
        self.sourceFilePath = sourceFilePath
        self.schemaPath = schemaPath
        self.files = [files]
        self.inputdateColumn = inputdateColumn
        self.outputdateColumn = outputdateColumn
        self.mnthOrdaily = mnthOrdaily
        self.noOfdaysOrMonth = noOfdaysOrMonth

        schemaFilePath = os.path.abspath(self.schemaPath)
        if os.path.exists(schemaPath):
            schema = SchemaReader.structTypemapping(schemaFilePath)
        checkSumColumns = DataTranformation.getCheckSumColumns(schemaFilePath)
        sparkSession = SparkSessionBuilder.sparkSessionBuild(self.appname)
        file_path = os.path.abspath(sourceFilePath)
        file_list = ['/SMS_2019090200.cdr']
        df_source = DataTranformation.readSourceFile(sparkSession, file_path, schema, checkSumColumns, file_list)

        # df_final = df_source.withColumn("outputColumn", py_function.substring(py_function.col(self.inputdateColumn), 0, 8)).show(10)

        date_range = DataTranformation.getCheckDate(self.mnthOrdaily, self.noOfdaysOrMonth)
        lateOrNormalCdr = DataTranformation.getLateOrNormalCdr(sparkSession, df_source, self.inputdateColumn, self.outputdateColumn, date_range)
        lateOrNormalCdr.show(10)
