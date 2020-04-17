
from lycaSparkTransformation.DataTranformation import DataTranformation
from lycaSparkTransformation.SchemaReader import SchemaReader
from lycaSparkTransformation.SparkSessionBuilder import SparkSessionBuilder
import os


class TransformActionChain:
    def __init__(self, module, subModule, ap, sourceFilePath, schemaPath, files):
        self.module = module
        self.subModule = subModule
        self.appname = ap
        self.sourceFilePath = sourceFilePath
        self.schemaPath = schemaPath
        self.files = [files]

        print("Reading schema file")

        schemaFilePath = os.path.abspath(self.schemaPath)
        if os.path.exists(schemaPath):
            schema = SchemaReader.structTypemapping(schemaFilePath)
        checkSumColumns = DataTranformation.getCheckSumColumns(schemaFilePath)
        sparkSession = SparkSessionBuilder.sparkSessionBuild(self.appname)
        file_path = os.path.abspath(sourceFilePath)
        file_list = ['/SMS_2019090200.cdr']
        df_merge = DataTranformation.readSourceFile(sparkSession, file_path, schema, checkSumColumns, file_list)
        df_merge.show(10)
