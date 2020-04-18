from lycaSparkTransformation.DataTranformation import DataTranformation
from lycaSparkTransformation.SchemaReader import SchemaReader
from lycaSparkTransformation.SparkSessionBuilder import SparkSessionBuilder
from pyspark.sql.types import IntegerType, StringType
import os


class TransformActionChain:
    def __init__(self, module, subModule, ap, sourceFilePath, schemaPath, files, dateColumn, formattedDateColumn,
                 integerDateColumn, mnthOrdaily, noOfdaysOrMonth):
        self.module = module
        self.subModule = subModule
        self.appname = ap
        self.sourceFilePath = sourceFilePath
        self.schemaPath = schemaPath
        self.files = [files]
        self.dateColumn = dateColumn
        self.formattedDateColumn = formattedDateColumn
        self.integerDateColumn = integerDateColumn
        self.mnthOrdaily = mnthOrdaily
        self.noOfdaysOrMonth = noOfdaysOrMonth

        schemaFilePath = os.path.abspath(self.schemaPath)
        if os.path.exists(schemaPath):
            schema = SchemaReader.structTypemapping(schemaFilePath)
        checkSumColumns = DataTranformation.getCheckSumColumns(schemaFilePath)
        sparkSession = SparkSessionBuilder.sparkSessionBuild(self.appname)
        file_path = os.path.abspath(sourceFilePath)
        file_list = ['/sample.cdr']
        df_source = DataTranformation.readSourceFile(sparkSession, file_path, schema, checkSumColumns, file_list)
        date_range = int(DataTranformation.getPrevRangeDate(self.mnthOrdaily, self.noOfdaysOrMonth))
        lateOrNormalCdr = DataTranformation.getLateOrNormalCdr(df_source, self.dateColumn, self.formattedDateColumn,
                                                               self.integerDateColumn, date_range)
        df_duplicate = DataTranformation.getDuplicates(df_source, "checksum")
        # df_duplicate.show(10, False)
        df_unique = DataTranformation.getUnique(df_source, "checksum")
        # df_unique.show(10)
        DataTranformation.writeToS3(df_unique)