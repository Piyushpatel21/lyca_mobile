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
        lateOrNormalCdr = DataTransformation.getLateOrNormalCdr(df_source, self.dateColumn, self.formattedDateColumn, self.integerDateColumn, date_range)
        df_duplicate = DataTransformation.getDuplicates(lateOrNormalCdr, "checksum")
        df_unique_late = DataTransformation.getUnique(lateOrNormalCdr, "checksum").filter("normalOrlate == 'Late'")
        df_unique_normal = DataTransformation.getUnique(lateOrNormalCdr, "checksum").filter("normalOrlate == 'Normal'")
        dfLateCDRNewRecord = DataTransformation.getDbDuplicate(df_unique_late, df_source).filter("newOrDupl == 'New'")
        dfLateCDRDuplicate = DataTransformation.getDbDuplicate(df_unique_late, df_source).filter("newOrDupl == 'Duplicate'")
        dfNormalCDRNewRecord = DataTransformation.getDbDuplicate(df_unique_normal, df_source).filter("newOrDupl == 'New'")
        dfNormalCDRDuplicate = DataTransformation.getDbDuplicate(df_unique_normal, df_source).filter("newOrDupl == 'Duplicate'")
        df_duplicate.show(10, False)
        dfLateCDRNewRecord.show(10, False)
        dfLateCDRDuplicate.show(10, False)
        dfNormalCDRNewRecord.show(10, False)
        dfNormalCDRDuplicate.show(10, False)