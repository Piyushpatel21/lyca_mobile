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
from pyspark.sql.types import IntegerType
from datetime import datetime
from lycaSparkTransformation.DataTransformation import DataTransformation, VoiceDataTransformation, GprsDataTransformation
from lycaSparkTransformation.SchemaReader import SchemaReader
from pyspark.sql import DataFrame
from lycaSparkTransformation.JSONBuilder import JSONBuilder
from awsUtils.RedshiftUtils import RedshiftUtils
from awsUtils.AwsReader import AwsReader
from pyspark.sql import functions as py_function


class TransformActionChain:
    def __init__(self, sparkSession: SparkSession, logger, module, subModule, configfile, connfile, run_date, prevDate, code_bucket):
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
        self.jsonParser = JSONBuilder(self.module, self.subModule, self.configfile, self.connfile)
        self.property = self.jsonParser.getAppPrpperty()
        self.connpropery = self.jsonParser.getConnPrpperty()

        self.redshiftprop = RedshiftUtils(self.connpropery.get("host"), self.connpropery.get("port"), self.connpropery.get("domain"),
                                          self.connpropery.get("user"), self.connpropery.get("password"), self.connpropery.get("tmpdir"))
        self.batch_start_dt = datetime.now()


    def getBatchID(self) -> int:
        return self.redshiftprop.getBatchId(self.sparkSession, self.subModule.upper(), self.prevDate)

    def srcSchema(self):
        try:
            self.logger.info("***** generating src, target and checksum schema *****")
            srcSchemaFilePath = AwsReader.s3ReadFile('s3', self.code_bucket, self.property.get("srcSchemaPath"))
            schema = SchemaReader.structTypemapping(srcSchemaFilePath)
            checkSumColumns = self.trans.getCheckSumColumns(srcSchemaFilePath)
            tgtSchemaPath = AwsReader.s3ReadFile('s3', self.code_bucket, self.property.get("tgtSchemaPath"))
            tgtColumns = self.trans.getTgtColumns(tgtSchemaPath)
            self.logger.info("***** return src, target and checksum schema *****")
            return {
                "srcSchema": schema,
                "checkSumColumns": checkSumColumns,
                "tgtSchema": tgtColumns
            }
        except Exception as ex:
            self.logger.error("Failed to create src, tgt, cheksum schema : {error}".format(error=ex))

    def getSourceData(self, batchid, srcSchema, checkSumColumns) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
        self.logger.info("***** reading source data from s3 *****")
        file_list = self.redshiftprop.getFileList(self.sparkSession, batchid)
        path = self.property.get("sourceFilePath") + "/" + self.module.upper() + "/" + "UK" + "/" +self.subModule.upper() + "/" + self.run_date[:4] + "/" + self.run_date[4:6] + "/" + self.run_date[6:8] + "/"
        try:
            df_source_raw = self.trans.readSourceFile(self.sparkSession, path, srcSchema, batchid, checkSumColumns, file_list)
            if self.property.get("subModule") == "gprs":
                gprsModuleTransformation = GprsDataTransformation()
                df_source_with_datatype = gprsModuleTransformation.convertTargetDataType(df_source_raw, srcSchema)
                df_source = gprsModuleTransformation.generateDerivedColumnsForGprs(df_source_with_datatype)
            elif self.property.get("subModule") == "voice":
                voiceModuleTransformation = VoiceDataTransformation()
                df_source_with_datatype = voiceModuleTransformation.convertTargetDataType(df_source_raw, srcSchema)
                df_source = voiceModuleTransformation.generateDerivedColumnsForVoice(df_source_with_datatype)
            else:
                df_source = df_source_raw
            s3_batchreadcount = df_source.agg(py_function.count('batch_id').cast(IntegerType()).alias('s3_batchreadcount')).rdd.flatMap(lambda row: row).collect()
            s3_filecount = df_source.agg(py_function.countDistinct('filename').cast(IntegerType()).alias('s3_filecount')).rdd.flatMap(lambda row: row).collect()
            batch_status = 'Started'
            metaQuery = ("INSERT INTO uk_rrbs_dm.log_batch_status_mno (BATCH_ID, S3_BATCHREADCOUNT, S3_FILECOUNT, BATCH_STATUS, BATCH_START_DT) values({batch_id},{s3_batchreadcount},{s3_filecount},'{batch_status}','{batch_start_dt}')"
                         .format(batch_id=batchid, s3_batchreadcount=''.join(str(e) for e in s3_batchreadcount), s3_filecount=''.join(str(e) for e in s3_filecount), batch_status=batch_status, batch_start_dt=self.batch_start_dt))
            self.redshiftprop.writeBatchStatus(self.sparkSession, metaQuery)
            date_range = int(self.trans.getPrevRangeDate(self.run_date, self.property.get("normalcdrfrq"), self.property.get("numofdayormnthnormal")))
            lateOrNormalCdr = self.trans.getLateOrNormalCdr(df_source, self.property.get("integerDateColumn"), date_range)
            df_duplicate = self.trans.getDuplicates(lateOrNormalCdr, "rec_checksum")
            batch_status = 'In-Progress'
            intrabatch_dupl_count = df_duplicate.agg(py_function.count('batch_id').cast(IntegerType()).alias('INTRABATCH_DUPL_COUNT')).rdd.flatMap(lambda row: row).collect()

            metaQuery = ("update uk_rrbs_dm.log_batch_status_mno set INTRABATCH_DEDUPL_STATUS='Complete', INTRABATCH_DUPL_COUNT={intrabatch_dupl_count}, BATCH_STATUS='{batch_status}', INTRABATCH_DIST_DUPL_COUNT={intrabatch_dist_dupl_count} where BATCH_ID={batch_id} and BATCH_END_DT is null"
                .format(batch_id=batchid, batch_status=batch_status, intrabatch_dupl_count=''.join(str(e) for e in intrabatch_dupl_count), intrabatch_dist_dupl_count=intrabatch_dist_dupl_count))
            self.redshiftprop.writeBatchStatus(self.sparkSession, metaQuery)
            df_unique_late = self.trans.getUnique(lateOrNormalCdr, "rec_checksum").filter("normalOrlate == 'Late'")
            intrabatch_late_count = df_unique_late.agg(py_function.count('batch_id').cast(IntegerType()).alias('INTRABATCH_NEW_LATE_COUNT')).rdd.flatMap(lambda row: row).collect()
            metaQuery = ("update uk_rrbs_dm.log_batch_status_mno set INTRABATCH_NEW_LATE_COUNT={intrabatch_late_count} where batch_id={batch_id} and batch_end_dt is null".format(batch_id=batchid, intrabatch_late_count=''.join(str(e) for e in intrabatch_late_count)))
            self.redshiftprop.writeBatchStatus(self.sparkSession, metaQuery)
            df_unique_normal = self.trans.getUnique(lateOrNormalCdr, "rec_checksum").filter("normalOrlate == 'Normal'")
            intrabatch_new_count = df_unique_normal.agg(py_function.count('batch_id').cast(IntegerType()).alias('INTRABATCH_NEW_NORMAL_COUNT')).rdd.flatMap(lambda row: row).collect()
            intrabatch_status = 'Complete'
            metaQuery = ("update uk_rrbs_dm.log_batch_status_mno set INTRABATCH_NEW_NORMAL_COUNT={intrabatch_new_count}, INTRABATCH_DEDUPL_STATUS='{intrabatch_status}' where BATCH_ID={batch_id} and BATCH_END_DT is null".format(batch_id=batchid, intrabatch_status=intrabatch_status, intrabatch_new_count=''.join(str(e) for e in intrabatch_new_count)))
            self.redshiftprop.writeBatchStatus(self.sparkSession, metaQuery)
            self.logger.info("***** source data prepared for transformation *****")
            record_count = df_source.groupBy('filename').agg(py_function.count('batch_id').cast(IntegerType()).alias('RECORD_COUNT'))
            return df_duplicate, df_unique_late, df_unique_normal, record_count
        except Exception as ex:
            metaQuery = ("INSERT INTO uk_rrbs_dm.log_batch_status_mno (batch_id,batch_status, batch_start_dt, batch_end_dt) values({batch_id}, '{batch_status}', '{batch_start_dt}', '{batch_end_dt}')".format(batch_id=batchid, batch_status='Failed', batch_start_dt=self.batch_start_dt, batch_end_dt=datetime.now()))
            self.redshiftprop.writeBatchStatus(self.sparkSession, metaQuery)
            self.logger.error("Failed to create source data : {error}".format(error=ex))

    def getDbDuplicate(self) -> Tuple[DataFrame, DataFrame]:
        try:
            self.logger.info("***** reading redshift data to check duplicate *****")
            normalDateRng = int(self.trans.getPrevRangeDate(self.run_date, self.property.get("normalcdrfrq"), self.property.get("numofdayormnthnormal")))
            lateDateRng = int(self.trans.getPrevRangeDate(self.run_date, self.property.get("latecdrfrq"), self.property.get("numofdayormnthlate")))
            dfDB = self.redshiftprop.readFromRedshift(self.sparkSession, self.property.get("database"), self.property.get("normalcdrtbl"))
            dfNormalDB = dfDB.filter(dfDB[self.property.get("integerDateColumn")] >= normalDateRng)
            dfLateDB = dfDB.filter(dfDB[self.property.get("integerDateColumn")] >= lateDateRng)
            self.logger.info("***** mart data is available for duplicate check *****")
            return dfNormalDB, dfLateDB
        except Exception as ex:
            self.logger.error("Failed to read redshift for db duplicate : {error}".format(error=ex))

    def getLateCDR(self, srcDataFrame: DataFrame, lateDBDataFrame: DataFrame, batchid) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
        try:
            self.logger.info("***** generating data for late cdr *****")
            dfLateCDRND = self.trans.checkDuplicate(srcDataFrame, lateDBDataFrame)
            dfLateCDRNewRecord = dfLateCDRND.filter("newOrDupl == 'New'")
            dfLateCDRDuplicate = dfLateCDRND.filter("newOrDupl == 'Duplicate'")
            self.logger.info("***** generating data for late cdr - completed *****")
            latecdr_count = dfLateCDRNewRecord.groupBy('filename').agg(py_function.count('batch_id').cast(IntegerType()).alias('DM_LATECDR_COUNT')
                                                                      ,py_function.count('batch_id').cast(IntegerType()).alias('LDM_LATECDR_COUNT'))
            latecdr_dupl_count = dfLateCDRDuplicate.groupBy('filename').agg(py_function.count('batch_id').cast(IntegerType()).alias('DM_LATECDR_DBDUPL_COUNT'))
            ldm_latecdr_count = dfLateCDRNewRecord.agg(py_function.count('batch_id').cast(IntegerType()).alias('LDM_LATECDR_COUNT')).rdd.flatMap(lambda row: row).collect()
            ldm_latecdr_dupl_count = dfLateCDRDuplicate.agg(py_function.count('batch_id').cast(IntegerType()).alias('LDM_LATECDR_DBDUPL_COUNT')).rdd.flatMap(lambda row: row).collect()
            ldm_latecdr_status = 'Complete'
            metaQuery = ("update uk_rrbs_dm.log_batch_status_mno set LDM_LATECDR_STATUS='{ldm_latecdr_status}', LDM_LATECDR_COUNT={ldm_latecdr_count}, LDM_LATECDR_DBDUPL_COUNT={ldm_latecdr_dupl_count} where BATCH_ID={batch_id} and BATCH_END_DT is null"
                        .format(batch_id=batchid, ldm_latecdr_status=ldm_latecdr_status, ldm_latecdr_count=''.join(str(e) for e in ldm_latecdr_count), ldm_latecdr_dupl_count=''.join(str(e) for e in ldm_latecdr_dupl_count)))
            self.redshiftprop.writeBatchStatus(self.sparkSession, metaQuery)
            return dfLateCDRNewRecord, dfLateCDRDuplicate, latecdr_count, latecdr_dupl_count
        except Exception as ex:
            self.logger.error("Failed to compute Late CDR data : {error}".format(error=ex))

    def getNormalCDR(self, srcDataFrame: DataFrame, normalDBDataFrame: DataFrame, batchid) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
        try:
            self.logger.info("***** generating data for normal cdr *****")
            dfNormalCDRND = self.trans.checkDuplicate(srcDataFrame, normalDBDataFrame)
            dfNormalCDRNewRecord = dfNormalCDRND.filter("newOrDupl == 'New'")
            dfNormalCDRDuplicate = dfNormalCDRND.filter("newOrDupl == 'Duplicate'")
            self.logger.info("***** generating data for normal cdr - completed *****")
            normalcdr_count = dfNormalCDRNewRecord.groupBy('filename').agg(py_function.count('batch_id').cast(IntegerType()).alias('DM_NORMAL_count'))
            normalcdr_dupl_count = dfNormalCDRDuplicate.groupBy('filename').agg(py_function.count('batch_id').cast(IntegerType()).alias('DM_NORMAL_DBDUPL_COUNT'))
            dm_normal_count = dfNormalCDRNewRecord.agg(py_function.count('batch_id').cast(IntegerType()).alias('DM_NORMAL_count')).rdd.flatMap(lambda row: row).collect()
            dm_normal_dupl_count = dfNormalCDRDuplicate.agg(py_function.count('batch_id').cast(IntegerType()).alias('DM_NORMAL_DBDUPL_COUNT')).rdd.flatMap(lambda row: row).collect()
            dm_normal_status = 'Complete'
            metaQuery = ("update uk_rrbs_dm.log_batch_status_mno set DM_NORMAL_STATUS='{dm_normal_status}', DM_NORMAL_COUNT={dm_normal_count}, DM_NORMAL_DBDUPL_COUNT={dm_normal_dupl_count} where BATCH_ID={batch_id} and BATCH_END_DT is null"
                .format(batch_id=batchid, dm_normal_status=dm_normal_status, dm_normal_count=''.join(str(e) for e in dm_normal_count),dm_normal_dupl_count=''.join(str(e) for e in dm_normal_dupl_count)))
            self.redshiftprop.writeBatchStatus(self.sparkSession, metaQuery)
            return dfNormalCDRNewRecord, dfNormalCDRDuplicate, normalcdr_count, normalcdr_dupl_count
        except Exception as ex:
            self.logger.error("Failed to compute Normal CDR data : {error}".format(error=ex))

    def writetoDataMart(self,srcDataframe: DataFrame, tgtColmns=[]):
        try:
            self.logger.info("***** started writing to data mart *****")
            df = srcDataframe.select(*tgtColmns)
            self.redshiftprop.writeToRedshift(df, self.property.get("database"), self.property.get("normalcdrtbl"), tgtColmns)
            self.logger.info("***** started writing to data mart - completed *****")
        except Exception as ex:
            self.logger.error("Failed to write data in data mart : {error}".format(error=ex))

    def writetoDuplicateCDR(self, dataframe: DataFrame, tgtColmns=[]):
        try:
            self.logger.info("***** started writing to duplicate mart *****")
            df = dataframe.select(*tgtColmns)
            self.redshiftprop.writeToRedshift(df, self.property.get("database"), self.property.get("duplicatecdrtbl"), tgtColmns)
            self.logger.info("***** started writing to duplicate mart - completed *****")
        except Exception as ex:
            self.logger.error("Failed to write data in duplicate mart : {error}".format(error=ex))

    def writetoLateCDR(self, srcDataframe: DataFrame, tgtColmns=[]):
        try:
            self.logger.info("***** started writing to late mart *****")
            df = srcDataframe.select(*tgtColmns)
            self.redshiftprop.writeToRedshift(df, self.property.get("database"), self.property.get("latecdrtbl"), tgtColmns)
            self.logger.info("***** started writing to late mart - completed *****")
        except Exception as ex:
            self.logger.error("Failed to write data in late mart : {error}".format(error=ex))

    def writeBatchFileStatus(self, dataframe : DataFrame, batch_id):
        try:
            self.logger.info("Writing batch status metadata")
            self.redshiftprop.writeBatchFileStatus(self.sparkSession, dataframe, batch_id)
            self.logger.info("Writing batch status metadata - completed")
        except Exception as ex:
            self.logger.error("Failed to write batch status metadata: {error}".format(error=ex))

    def writeBatchStatus(self, batch_id, status):
        batch_end_dt = datetime.now()
        batch_status = status
        metaQuery = ("update uk_rrbs_dm.log_batch_status_mno set BATCH_STATUS='{batch_status}', BATCH_END_DT='{batch_end_dt}' where BATCH_ID = {batch_id} and BATCH_END_DT is null"
            .format(batch_status=batch_status, batch_end_dt=batch_end_dt, batch_id=batch_id))
        self.redshiftprop.writeBatchStatus(self.sparkSession, metaQuery)

