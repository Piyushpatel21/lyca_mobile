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
from datetime import datetime
from lycaSparkTransformation.DataTransformation import DataTransformation
from pyspark.sql import DataFrame
from lycaSparkTransformation.JSONBuilder import JSONBuilder
from awsUtils.RedshiftUtils import RedshiftUtils, IntegerType
from awsUtils.AwsReader import AwsReader
from pyspark.sql import functions as F


class TransformActionChain:
    def __init__(self, sparkSession: SparkSession, logger, module, subModule, configfile, connfile, run_date, prevDate, code_bucket, encoding):
        self.sparkSession = sparkSession
        self.logger = logger
        self.module = module
        self.subModule = subModule
        self.code_bucket = code_bucket
        self.configfile = AwsReader.s3ReadFile('s3', self.code_bucket, configfile)
        self.connfile = AwsReader.s3ReadFile('s3', self.code_bucket, connfile)
        self.trans = DataTransformation()
        self.run_date = run_date
        self.prevDate = prevDate
        self.jsonParser = JSONBuilder(self.configfile, self.connfile)
        self.property = self.jsonParser.getAppPrpperty()
        self.connpropery = self.jsonParser.getConnPrpperty()
        self.redshiftprop = RedshiftUtils(self.connpropery.get("host"), self.connpropery.get("port"), self.connpropery.get("domain"),
                                          self.connpropery.get("user"), self.connpropery.get("password"), self.connpropery.get("tmpdir"))
        self.batch_start_dt = datetime.now()
        self.logBatchFileTbl = ".".join([self.property.get("logdb"), self.property.get("batchfiletbl")])
        self.logBatchStatusTbl = ".".join([self.property.get("logdb"), self.property.get("batchstatustbl")])
        self.encoding = encoding

    def getBatchID(self, ) -> int:
        return self.redshiftprop.getBatchId(self.sparkSession, self.logBatchFileTbl, self.subModule.upper(), self.prevDate)

    def getSourceData(self, batchid) -> Tuple[DataFrame, DataFrame]:
        self.logger.info("***** reading source data from s3 *****")
        file_list = self.redshiftprop.getFileList(self.sparkSession, self.logBatchFileTbl, batchid)
        fList = []
        s3 = self.property.get("sourceFilePath")
        for file in file_list:
            fList.append(s3 + "/" + file)
        try:
            df_source = self.trans.readSourceFile(self.sparkSession, batchid, self.encoding, fList)
            s3_record_count = df_source.groupBy('recon_file_name').agg(F.count('recon_file_name').cast(IntegerType()).alias('s3_record_count'))
            s3_batchreadcount = df_source.count()
            s3_filecount = df_source.select('recon_file_name').distinct().count()
            batch_status = 'Started'
            metaQuery = ("INSERT INTO {log_batch_status} (batch_id, s3_batchreadcount, s3_filecount,batch_start_dt, batch_status) "
                         "values({batch_id}, {s3_batchreadcount}, {s3_filecount},{batch_start_dt}, {batch_status})"
                        .format(log_batch_status=self.logBatchStatusTbl, batch_id=batchid, s3_batchreadcount=s3_batchreadcount, s3_filecount=s3_filecount,
                                batch_start_dt=self.batch_start_dt, batch_status=batch_status))
            print(metaQuery)
            self.redshiftprop.writeBatchStatus(self.sparkSession, self.logBatchStatusTbl, metaQuery)
            return df_source, s3_record_count
        except Exception as ex:
            metaQuery = ("INSERT INTO {log_batch_status} (batch_id,batch_status, batch_start_dt, batch_end_dt) values({batch_id}, '{batch_status}', '{batch_start_dt}', '{batch_end_dt}')".format(log_batch_status=self.logBatchStatusTbl, batch_id=batchid, batch_status='Failed', batch_start_dt=self.batch_start_dt, batch_end_dt=datetime.now()))
            self.redshiftprop.writeBatchStatus(self.sparkSession, self.logBatchStatusTbl, metaQuery)
            self.logger.error("Failed to create source data : {error}".format(error=ex))

    def writetoDataMart(self, srcDataframe: DataFrame):
        try:
            tgtColmns = ''
            self.logger.info("***** started writing to data mart *****")
            self.redshiftprop.writeToRedshift(srcDataframe, self.property.get("database"), self.property.get("normalcdrtbl"), tgtColmns)
            self.logger.info("***** started writing to data mart - completed *****")
        except Exception as ex:
            self.logger.error("Failed to write data in data mart : {error}".format(error=ex))

    def writeBatchFileStatus(self, dataframe : DataFrame, batch_id):
        try:
            self.logger.info("Writing batch status metadata")
            self.redshiftprop.writeBatchFileStatus(self.sparkSession, self.logBatchFileTbl, dataframe, batch_id)
            self.logger.info("Writing batch status metadata - completed")
        except Exception as ex:
            self.logger.error("Failed to write batch status metadata: {error}".format(error=ex))

    def writeBatchStatus(self, batch_id, status):
        batch_end_dt = datetime.now()
        batch_status = status
        metaQuery = ("update {log_batch_status} set BATCH_STATUS='{batch_status}', BATCH_END_DT='{batch_end_dt}' where BATCH_ID = {batch_id} and BATCH_END_DT is null"
            .format(log_batch_status=self.logBatchStatusTbl, batch_status=batch_status, batch_end_dt=batch_end_dt, batch_id=batch_id))
        self.redshiftprop.writeBatchStatus(self.sparkSession, self.logBatchStatusTbl, metaQuery)

