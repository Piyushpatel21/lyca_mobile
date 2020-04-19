from lycaSparkTransformation.DataTransformation import DataTransformation
from lycaSparkTransformation.SchemaReader import SchemaReader
from lycaSparkTransformation.SparkSessionBuilder import SparkSessionBuilder
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
        checkSumColumns = DataTransformation.getCheckSumColumns(schemaFilePath)
        sparkSession = SparkSessionBuilder.sparkSessionBuild(self.appname)
        file_path = os.path.abspath(sourceFilePath)
        file_list = ['/sample.cdr']
        df_source = DataTransformation.readSourceFile(sparkSession, file_path, schema, checkSumColumns, file_list)
        date_range = int(DataTransformation.getPrevRangeDate(self.mnthOrdaily, self.noOfdaysOrMonth))
        lateOrNormalCdr = DataTransformation.getLateOrNormalCdr(df_source, self.dateColumn, self.formattedDateColumn,
                                                               self.integerDateColumn, date_range)
        df_duplicate = DataTransformation.getDuplicates(df_source, "checksum")
        # df_duplicate.show(10, False)
        df_unique = DataTransformation.getUnique(df_source, "checksum")
        # df_unique.show(10)
        DataTransformation.writeToS3(df_unique)