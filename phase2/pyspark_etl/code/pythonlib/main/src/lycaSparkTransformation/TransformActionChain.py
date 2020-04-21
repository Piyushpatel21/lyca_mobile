########################################################################
# description     : processing JSON config files.                      #
# author          : Naren K(narendra.kumar@cloudwick.com),             #
#                   Tejveer Singh(tejveer.singh@cloudwick.com)         #
#                   Shubhajit Saha(shubhajit.saha@cloudwick.com)       #
# contributor     :                                                    #
# version         : 1.0                                                #
# notes           :                                                    #
########################################################################

from lycaSparkTransformation.DataTransformation import DataTransformation
from lycaSparkTransformation.SchemaReader import SchemaReader
from lycaSparkTransformation.SparkSessionBuilder import SparkSessionBuilder
import os


class TransformActionChain:
    def __init__(self, module, subModule, ap, sourceFilePath, srcSchemaPath, tgtSchemaPath, files, dateColumn, formattedDateColumn,
                 integerDateColumn, mnthOrdaily, noOfdaysOrMonth):
        self.module = module
        self.subModule = subModule
        self.appname = ap
        self.sourceFilePath = sourceFilePath
        self.srcSchemaPath = srcSchemaPath
        self.tgtSchemaPath = tgtSchemaPath
        self.files = [files]
        self.dateColumn = dateColumn
        self.formattedDateColumn = formattedDateColumn
        self.integerDateColumn = integerDateColumn
        self.mnthOrdaily = mnthOrdaily
        self.noOfdaysOrMonth = noOfdaysOrMonth

        srcSchemaFilePath = os.path.abspath(self.srcSchemaPath)
        if os.path.exists(srcSchemaPath):
            schema = SchemaReader.structTypemapping(self.srcSchemaPath)
        checkSumColumns = DataTransformation.getCheckSumColumns(srcSchemaFilePath)
        tgtColumns = DataTransformation.getTgtColumns(self.tgtSchemaPath)
        sparkSession = SparkSessionBuilder.sparkSessionBuild(self.appname)
        sparkSession.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        sparkSession.conf.set("spark.hadoop.mapred.output.committer.class","com.appsflyer.spark.DirectOutputCommitter")

        file_path = os.path.abspath(sourceFilePath)
        file_list = ['/sample2.csv']
        run_date = '20200420'
        df_source = DataTransformation.readSourceFile(sparkSession, file_path, schema, checkSumColumns, file_list)
        df_lateDB = sparkSession.read.option("header", "true").csv('../../../../pythonlib/test/resources/output/20200420/dataMart/')
        df_normalDB = sparkSession.read.option("header", "true").csv('../../../../pythonlib/test/resources/output/20200420/dataMart/')
        date_range = int(DataTransformation.getPrevRangeDate(self.mnthOrdaily, self.noOfdaysOrMonth))
        lateOrNormalCdr = DataTransformation.getLateOrNormalCdr(df_source, self.dateColumn, self.formattedDateColumn, self.integerDateColumn, date_range)
        df_duplicate = DataTransformation.getDuplicates(lateOrNormalCdr, "checksum")
        df_unique_late = DataTransformation.getUnique(lateOrNormalCdr, "checksum").filter("normalOrlate == 'Late'")
        df_unique_normal = DataTransformation.getUnique(lateOrNormalCdr, "checksum").filter("normalOrlate == 'Normal'")
        dfLateCDRNewRecord = DataTransformation.getDbDuplicate(df_unique_late, df_lateDB).filter("newOrDupl == 'New'")
        dfLateCDRDuplicate = DataTransformation.getDbDuplicate(df_unique_late, df_lateDB).filter("newOrDupl == 'Duplicate'")
        dfNormalCDRNewRecord = DataTransformation.getDbDuplicate(df_unique_normal, df_normalDB).filter("newOrDupl == 'New'")
        dfNormalCDRDuplicate = DataTransformation.getDbDuplicate(df_unique_normal, df_normalDB).filter("newOrDupl == 'Duplicate'")
        print("source file duplicate ============>")
        df_duplicate.show(20, False)
        print("unique late record ============>")
        df_unique_late.show(20, False)
        print("unique normal record ============>")
        df_unique_normal.show(20, False)
        print("Late DB New Record ============>")
        dfLateCDRNewRecord.show(20, False)
        print("Late DB Duplicate Record ============>")
        dfLateCDRDuplicate.show(20, False)
        print("Normal DB New Record ============>")
        dfNormalCDRNewRecord.show(20, False)
        print("Normal DB Duplicate Record ============>")
        dfNormalCDRDuplicate.show(150, False)
        DataTransformation.writeToS3(dfLateCDRNewRecord, run_date, 'dataMart', 'normalDB.csv', tgtColumns)
        DataTransformation.writeToS3(df_duplicate, run_date, 'duplicateModel', 'duplicate.csv')
        DataTransformation.writeToS3(dfLateCDRNewRecord, run_date, 'lateCDR', 'late.csv')
        DataTransformation.writeToS3(dfLateCDRDuplicate, run_date, 'duplicateModel', 'duplicate.csv')
        DataTransformation.writeToS3(dfNormalCDRNewRecord, run_date, 'dataMart', 'normalDB.csv')
        DataTransformation.writeToS3(dfNormalCDRDuplicate, run_date, 'duplicateModel', 'duplicate.csv')