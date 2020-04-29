########################################################################
# description     : processing JSON config files.                      #
# author          : Naren K(narendra.kumar@cloudwick.com),             #
#                   Tejveer Singh(tejveer.singh@cloudwick.com)         #
#                   Shubhajit Saha(shubhajit.saha@cloudwick.com)       #
# contributor     :                                                    #
# version         : 1.0                                                #
# notes           :                                                    #
########################################################################
from typing import Tuple
from pyspark.sql import SparkSession

from lycaSparkTransformation.DataTransformation import DataTransformation
from lycaSparkTransformation.SchemaReader import SchemaReader
from pyspark.sql import DataFrame
import os
from lycaSparkTransformation.JSONBuilder import JSONBuilder


class TransformActionChain:
    def __init__(self, logger, module, subModule, filePath):
        self.logger = logger
        self.module = module
        self.subModule = subModule
        self.filePath = filePath
        self.property = JSONBuilder(self.module, self.subModule, self.filePath).getPrpperty()
        self.logger.info("We are in action chain")
    def srcSchema(self):
        try:
            self.logger.info("something")
            srcSchemaFilePath = os.path.abspath(self.property.get("srcSchemaPath"))
            schema = SchemaReader.structTypemapping(self.property.get("srcSchemaPath"))
            checkSumColumns = DataTransformation.getCheckSumColumns(srcSchemaFilePath)
            tgtColumns = DataTransformation.getTgtColumns(self.property.get("tgtSchemaPath"))
            return {
                "srcSchema": schema,
                "checkSumColumns": checkSumColumns,
                "tgtSchema": tgtColumns
            }
        except ValueError:
            "Error"

    def getSourceData(self, sparkSession: SparkSession, srcSchema, checkSumColumns, file_list, run_date) -> Tuple[DataFrame, DataFrame, DataFrame]:
        try:
            df_source = DataTransformation.readSourceFile(sparkSession, self.property.get("sourceFilePath"), srcSchema, checkSumColumns, file_list)
            date_range = int(DataTransformation.getPrevRangeDate(self.property.get("mnthOrdaily"), self.property.get("noOfdaysOrMonth")))
            lateOrNormalCdr = DataTransformation.getLateOrNormalCdr(df_source, self.property.get("dateColumn"), self.property.get("formattedDateColumn"), self.property.get("integerDateColumn"), date_range)
            df_duplicate = DataTransformation.getDuplicates(lateOrNormalCdr, "checksum")
            df_unique_late = DataTransformation.getUnique(lateOrNormalCdr, "checksum").filter("normalOrlate == 'Late'")
            df_unique_normal = DataTransformation.getUnique(lateOrNormalCdr, "checksum").filter("normalOrlate == 'Normal'")
            return df_duplicate, df_unique_late, df_unique_normal
        except Exception:
            print("Error in DataFrame")

    def getDbDuplicate(self, sparkSession: SparkSession) -> Tuple[DataFrame, DataFrame]:
        try:
            dfDB = sparkSession.read.option("header", "true").csv('../../../../pythonlib/test/resources/output/20200420/dataMart/')
            normalDateRng = int(DataTransformation.getPrevRangeDate(self.property.get("mnthOrdaily"), self.property.get("noOfdaysOrMonth")))
            lateDateRng = int(DataTransformation.getPrevRangeDate(self.property.get("mnthOrdaily"), self.property.get("noOfdaysOrMonth")))
            dfNormalDB = dfDB.filter(dfDB[self.property.get("integerDateColumn")] <= normalDateRng)
            dfLateDB = dfDB.filter(dfDB[self.property.get("integerDateColumn")] <= lateDateRng)
            return dfNormalDB, dfLateDB
        except Exception:
            print("Error in DataFrame")

    def getLateCDR(self, srcDataFrame, lateDBDataFrame) -> Tuple[DataFrame, DataFrame]:
        try:
            dfLateCDRNewRecord = DataTransformation.getDbDuplicate(srcDataFrame, lateDBDataFrame).filter("newOrDupl == 'New'")
            dfLateCDRDuplicate = DataTransformation.getDbDuplicate(srcDataFrame, lateDBDataFrame).filter("newOrDupl == 'Duplicate'")
            return dfLateCDRNewRecord, dfLateCDRDuplicate
        except Exception as ex :
            print("Error in DataFrame")

    def getNormalCDR(self, srcDataFrame, normalDBDataFrame) -> Tuple[DataFrame, DataFrame]:
        try:
            dfNormalCDRNewRecord = DataTransformation.getDbDuplicate(srcDataFrame, normalDBDataFrame).filter("newOrDupl == 'New'")
            dfNormalCDRDuplicate = DataTransformation.getDbDuplicate(srcDataFrame, normalDBDataFrame).filter("newOrDupl == 'Duplicate'")
            return dfNormalCDRNewRecord, dfNormalCDRDuplicate
        except Exception:
            print("Error in DataFrame")

    def dfWrite(self, dataFrame: DataFrame, run_date, cdrType, fileName, tgtSchema):
        path = '../../../../pythonlib/test/resources/output/' + str(run_date) + '/' + cdrType + '/'
        print(os.path.abspath(path))
        DataTransformation.writeToS3(dataFrame, run_date, os.path.abspath(path), tgtSchema)