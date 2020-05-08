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
from awsUtils.RedshiftUtils import RedshiftUtils


class TransformActionChain:
    def __init__(self, logger, module, subModule, filePath, connfile, batchid, run_date):
        self.logger = logger
        self.module = module
        self.subModule = subModule
        self.filePath = filePath
        self.connfile = connfile
        self.batchid = batchid
        self.run_date = DataTransformation.getPrevRangeDate(run_date)
        self.jsonParser = JSONBuilder(self.module, self.subModule, self.filePath, self.connfile)
        self.property = self.jsonParser.getAppPrpperty()
        self.connpropery = self.jsonParser.getConnPrpperty()
        self.redshiftprop = RedshiftUtils(self.connpropery.get("host"), self.connpropery.get("port"), self.connpropery.get("user"), self.connpropery.get("domain"),
                                          self.connpropery.get("password"), self.connpropery.get("tmpdir"))

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

    def getSourceData(self, sparkSession: SparkSession, srcSchema, checkSumColumns) -> Tuple[DataFrame, DataFrame, DataFrame]:
        try:
            file_list = RedshiftUtils.getFileList(self.batchid)
            df_source = DataTransformation.readSourceFile(sparkSession, self.property.get("sourceFilePath"), srcSchema, str(self.batchid), checkSumColumns, file_list)
            date_range = int(DataTransformation.getPrevRangeDate(self.run_date, self.property.get("normalcdrfrq"), self.property.get("numofdayormnthnormal")))
            lateOrNormalCdr = DataTransformation.getLateOrNormalCdr(df_source, self.property.get("dateColumn"), self.property.get("formattedDateColumn"), self.property.get("integerDateColumn"), date_range)
            df_duplicate = DataTransformation.getDuplicates(lateOrNormalCdr, "checksum")
            df_unique_late = DataTransformation.getUnique(lateOrNormalCdr, "checksum").filter("normalOrlate == 'Late'")
            df_unique_normal = DataTransformation.getUnique(lateOrNormalCdr, "checksum").filter("normalOrlate == 'Normal'")
            return df_duplicate, df_unique_late, df_unique_normal
        except Exception as ex:
            print(ex)

    def getDbDuplicate(self, sparkSession: SparkSession) -> Tuple[DataFrame, DataFrame]:
        try:
            normalDateRng = int(DataTransformation.getPrevRangeDate(self.run_date, self.property.get("normalcdrfrq"), self.property.get("numofdayormnthnormal")))
            lateDateRng = int(DataTransformation.getPrevRangeDate(self.run_date, self.property.get("latecdrfrq"), self.property.get("numofdayormnthlate")))
            dfDB = RedshiftUtils.readFromRedshift(sparkSession, self.property.get("domain"), self.property.get("normalcdrtbl"))
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
        except Exception as ex:
            print("Error in DataFrame")

    def getNormalCDR(self, srcDataFrame, normalDBDataFrame) -> Tuple[DataFrame, DataFrame]:
        try:
            dfNormalCDRNewRecord = DataTransformation.getDbDuplicate(srcDataFrame, normalDBDataFrame).filter("newOrDupl == 'New'")
            dfNormalCDRDuplicate = DataTransformation.getDbDuplicate(srcDataFrame, normalDBDataFrame).filter("newOrDupl == 'Duplicate'")
            return dfNormalCDRNewRecord, dfNormalCDRDuplicate
        except Exception:
            print("Error in DataFrame")

    def writetoDataMart(self, dataframe, tgtColmns=[]):
        df = dataframe.select(*tgtColmns)
        RedshiftUtils.writeToRedshift(df, self.property.get("domain"), self.property.get("normalcdrtbl"))

    def writetoDuplicateCDR(self, dataframe, tgtColmns=[]):
        df = dataframe.select(*tgtColmns)
        RedshiftUtils.writeToRedshift(df, self.property.get("domain"), self.property.get("duplicatecdrtbl"))

    def writetoLateCDR(self, dataframe, tgtColmns=[]):
        df = dataframe.select(*tgtColmns)
        RedshiftUtils.writeToRedshift(df, self.property.get("domain"), self.property.get("latecdrtbl"))
