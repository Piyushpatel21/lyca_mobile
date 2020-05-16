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
from lycaSparkTransformation.JSONBuilder import JSONBuilder
from awsUtils.RedshiftUtils import RedshiftUtils
from awsUtils.AwsReader import AwsReader


class TransformActionChain:
    def __init__(self, logger, module, subModule, configfile, connfile, batchid, run_date):
        self.logger = logger
        self.module = module
        self.subModule = subModule
        self.configfile = AwsReader.s3ReadFile('s3', 'aws-glue-temporary-484320814466-eu-west-2', configfile)
        self.connfile = AwsReader.s3ReadFile('s3', 'aws-glue-temporary-484320814466-eu-west-2', connfile)
        self.batchid = batchid
        self.run_date = DataTransformation.getPrevRangeDate(run_date)
        self.jsonParser = JSONBuilder(self.module, self.subModule, self.configfile, self.connfile)
        self.property = self.jsonParser.getAppPrpperty()
        self.connpropery = self.jsonParser.getConnPrpperty()
        self.redshiftprop = RedshiftUtils(self.connpropery.get("host"), self.connpropery.get("port"), self.connpropery.get("domain"),
                                          self.connpropery.get("user"), self.connpropery.get("password"), self.connpropery.get("tmpdir"))

    def srcSchema(self):
        try:
            self.logger.info("generating src, target and checksum schema")
            srcSchemaFilePath = AwsReader.s3ReadFile('s3', 'aws-glue-temporary-484320814466-eu-west-2', self.property.get("srcSchemaPath"))
            schema = SchemaReader.structTypemapping(srcSchemaFilePath)
            checkSumColumns = DataTransformation.getCheckSumColumns(srcSchemaFilePath)
            tgtSchemaPath = AwsReader.s3ReadFile('s3', 'aws-glue-temporary-484320814466-eu-west-2', self.property.get("tgtSchemaPath"))
            tgtColumns = DataTransformation.getTgtColumns(tgtSchemaPath)
            return {
                "srcSchema": schema,
                "checkSumColumns": checkSumColumns,
                "tgtSchema": tgtColumns
            }
        except ValueError:
            "Error"

    def getSourceData(self, sparkSession: SparkSession, srcSchema, checkSumColumns) -> Tuple[DataFrame, DataFrame, DataFrame]:
        try:
            s3 = AwsReader.getawsClient('s3')
            file_list = ['UKR6_CS_08_05_2020_05_36_50_24934.cdr']
            # file_list = self.redshiftprop.getFileList(sparkSession)
            prmryKey = "sk_rrbs_" + self.subModule
            path = self.property.get("sourceFilePath") + "/" + self.module.upper() + "/" + "UK" + "/" +self.subModule.upper() + "/" + self.run_date[:4] + "/" + self.run_date[4:6] + "/" + self.run_date[6:8] + "/"
            df_source = DataTransformation.readSourceFile(sparkSession, path, srcSchema, str(self.batchid), prmryKey, checkSumColumns, file_list)
            date_range = int(DataTransformation.getPrevRangeDate(self.run_date, self.property.get("normalcdrfrq"), self.property.get("numofdayormnthnormal")))
            lateOrNormalCdr = DataTransformation.getLateOrNormalCdr(df_source, self.property.get("dateColumn"), self.property.get("formattedDateColumn"), self.property.get("integerDateColumn"), date_range)
            df_duplicate = DataTransformation.getDuplicates(lateOrNormalCdr, "rec_checksum")
            df_unique_late = DataTransformation.getUnique(lateOrNormalCdr, "rec_checksum").filter("normalOrlate == 'Late'")
            df_unique_normal = DataTransformation.getUnique(lateOrNormalCdr, "rec_checksum").filter("normalOrlate == 'Normal'")
            return df_duplicate, df_unique_late, df_unique_normal
        except Exception as ex:
            print(ex)

    def getDbDuplicate(self, sparkSession: SparkSession) -> Tuple[DataFrame, DataFrame]:
        try:
            normalDateRng = int(DataTransformation.getPrevRangeDate(self.run_date, self.property.get("normalcdrfrq"), self.property.get("numofdayormnthnormal")))
            print("normalDateRng" + str(normalDateRng))
            lateDateRng = int(DataTransformation.getPrevRangeDate(self.run_date, self.property.get("latecdrfrq"), self.property.get("numofdayormnthlate")))
            print("lateDateRng" + str(lateDateRng))
            print("DBNAME -> " + str(self.property.get("database")))
            print("NORMALCDRTBL -> " + str(self.property.get("normalcdrtbl")))
            dfDB = self.redshiftprop.readFromRedshift(sparkSession, self.property.get("database"), self.property.get("normalcdrtbl"))
            dfNormalDB = dfDB.filter(dfDB[self.property.get("integerDateColumn")] <= normalDateRng)
            dfLateDB = dfDB.filter(dfDB[self.property.get("integerDateColumn")] <= lateDateRng)
            self.logger.info("reading redshift data")
            dfDB.show(20, False)
            return dfNormalDB, dfLateDB
        except Exception as ex:
            self.logger.info("Failed to read redshift for db duplicate")
            self.logger.info(ex)

    def getLateCDR(self, srcDataFrame: DataFrame, lateDBDataFrame: DataFrame) -> Tuple[DataFrame, DataFrame]:
        try:
            dfLateCDRND = DataTransformation.getDbDuplicate(srcDataFrame, lateDBDataFrame)
            dfLateCDRNewRecord = dfLateCDRND.filter("newOrDupl == 'New'")
            dfLateCDRDuplicate = dfLateCDRND.filter("newOrDupl == 'Duplicate'")
            return dfLateCDRNewRecord, dfLateCDRDuplicate
        except Exception as ex:
            self.logger.info("Failed to compute Late CDR data")
            self.logger.info(ex)

    def getNormalCDR(self, srcDataFrame: DataFrame, normalDBDataFrame: DataFrame) -> Tuple[DataFrame, DataFrame]:
        try:
            self.logger.info("srcDataFrame data")
            srcDataFrame.show(20, False)
            self.logger.info("normalDBDataFrame data")
            normalDBDataFrame.show(20, False)
            dfNormalCDRND = DataTransformation.getDbDuplicate(srcDataFrame, normalDBDataFrame)
            self.logger.info("dfNormalCDRND data")
            dfNormalCDRND.show(20, False)
            dfNormalCDRNewRecord = dfNormalCDRND.filter("newOrDupl == 'New'")
            dfNormalCDRDuplicate = dfNormalCDRND.filter("newOrDupl == 'Duplicate'")
            return dfNormalCDRNewRecord, dfNormalCDRDuplicate
        except Exception as ex:
            self.logger.info("Failed to compute Normal CDR data")
            self.logger.info(ex)

    def writetoDataMart(self, dataframe: DataFrame, tgtColmns=[]):
        df = dataframe.select(*tgtColmns)
        self.logger.info("Writing in redshift table ===> " + self.property.get("database") + " " + self.property.get("normalcdrtbl"))
        self.redshiftprop.writeToRedshift(df, self.property.get("database"), self.property.get("normalcdrtbl"))

    def writetoDuplicateCDR(self, dataframe: DataFrame, tgtColmns=[]):
        df = dataframe.select(*tgtColmns)
        self.redshiftprop.writeToRedshift(df, self.property.get("database"), self.property.get("duplicatecdrtbl"))

    def writetoLateCDR(self, dataframe: DataFrame, tgtColmns=[]):
        df = dataframe.select(*tgtColmns)
        self.redshiftprop.writeToRedshift(df, self.property.get("database"), self.property.get("latecdrtbl"))
