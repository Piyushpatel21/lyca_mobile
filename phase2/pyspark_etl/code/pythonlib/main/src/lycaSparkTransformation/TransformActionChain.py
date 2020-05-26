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
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from datetime import datetime
from lycaSparkTransformation.DataTransformation import DataTransformation
from lycaSparkTransformation.SchemaReader import SchemaReader
from pyspark.sql import DataFrame
from lycaSparkTransformation.JSONBuilder import JSONBuilder
from awsUtils.RedshiftUtils import RedshiftUtils
from awsUtils.AwsReader import AwsReader
from pyspark.sql import functions as py_function


class TransformActionChain:
    def __init__(self, sparkSession: SparkSession, logger, module, subModule, configfile, connfile, run_date, batch_from=None, batch_to=None):
        self.sparkSession = sparkSession
        self.logger = logger
        self.module = module
        self.subModule = subModule
        self.batch_from = batch_from
        self.batch_to = batch_to
        self.configfile = AwsReader.s3ReadFile('s3', 'aws-glue-temporary-484320814466-eu-west-2', configfile)
        self.connfile = AwsReader.s3ReadFile('s3', 'aws-glue-temporary-484320814466-eu-west-2', connfile)
        self.trans = DataTransformation()
        self.run_date = run_date
        self.jsonParser = JSONBuilder(self.module, self.subModule, self.configfile, self.connfile)
        self.property = self.jsonParser.getAppPrpperty()
        self.connpropery = self.jsonParser.getConnPrpperty()
        self.redshiftprop = RedshiftUtils(self.connpropery.get("host"), self.connpropery.get("port"), self.connpropery.get("domain"),
                                          self.connpropery.get("user"), self.connpropery.get("password"), self.connpropery.get("tmpdir"))

    def getstatus(self, sparkSession: SparkSession, batch_ID: int, column, status) -> DataFrame:
        schema = StructType([StructField('batch_ID', IntegerType(), True), StructField(column, StringType(), True)])
        data = [(batch_ID, status)]
        rdd = sparkSession.sparkContext.parallelize(data)
        return sparkSession.createDataFrame(rdd, schema)

    def getdmCNT(self, sparkSession: SparkSession, batch_ID: int, column, cnt: int) -> DataFrame:
        schema = StructType([StructField('batch_ID', IntegerType(), True), StructField(column, IntegerType(), True)])
        data = [(batch_ID, cnt)]
        rdd = sparkSession.sparkContext.parallelize(data)
        return sparkSession.createDataFrame(rdd, schema)

    def getBatchID(self, sparkSession: SparkSession) -> int:
        return self.redshiftprop.getBatchId(sparkSession, self.subModule.upper(), self.batch_from, self.batch_to)

    def srcSchema(self):
        try:
            self.logger.info("***** generating src, target and checksum schema *****")
            srcSchemaFilePath = AwsReader.s3ReadFile('s3', 'aws-glue-temporary-484320814466-eu-west-2', self.property.get("srcSchemaPath"))
            schema = SchemaReader.structTypemapping(srcSchemaFilePath)
            checkSumColumns = self.trans.getCheckSumColumns(srcSchemaFilePath)
            tgtSchemaPath = AwsReader.s3ReadFile('s3', 'aws-glue-temporary-484320814466-eu-west-2', self.property.get("tgtSchemaPath"))
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
        try:
            batch_start_dt = datetime.now()
            self.logger.info("***** reading source data from s3 *****")
            # file_list = self.redshiftprop.getFileList(sparkSession, batchid)
            file_list = ['UKR6_CS_08_05_2020_04_15_58_24910.cdr']
            # path ='/Users/narenk/PycharmProjects/lycamobile-etl-movements/phase2/pyspark_etl/code/pythonlib/test/resources/'
            path = self.property.get("sourceFilePath") + "/" + self.module.upper() + "/" + "UK" + "/" +self.subModule.upper() + "/" + self.run_date[:4] + "/" + self.run_date[4:6] + "/" + self.run_date[6:8] + "/"
            df_source = self.trans.readSourceFile(self.sparkSession, path, srcSchema, batchid, checkSumColumns, file_list)
            s3_batchreadcount = df_source.agg(py_function.count('batch_id').cast(IntegerType()).alias('s3_batchreadcount')).rdd.flatMap(lambda row: row).collect()
            s3_filecount = df_source.agg(py_function.countDistinct('filename').cast(IntegerType()).alias('s3_filecount')).rdd.flatMap(lambda row: row).collect()
            batch_status = 'Started'
            metaQuery = ("INSERT INTO uk_rrbs_dm.log_batch_status_rrbs (batch_id, s3_batchreadcount, s3_filecount, batch_status, batch_start_dt) values({batch_id},{s3_batchreadcount},{s3_filecount},'{batch_status}','{batch_start_dt}')"
                         .format(batch_id=batchid, s3_batchreadcount=''.join(str(e) for e in s3_batchreadcount), s3_filecount=''.join(str(e) for e in s3_filecount), batch_status=batch_status, batch_start_dt=batch_start_dt))
            self.redshiftprop.writeBatchStatus(self.sparkSession, metaQuery)
            date_range = int(self.trans.getPrevRangeDate(self.run_date, self.property.get("normalcdrfrq"), self.property.get("numofdayormnthnormal")))
            lateOrNormalCdr = self.trans.getLateOrNormalCdr(df_source, self.property.get("dateColumn"), self.property.get("formattedDateColumn"), self.property.get("integerDateColumn"), date_range)
            df_duplicate = self.trans.getDuplicates(lateOrNormalCdr, "rec_checksum")
            batch_status = 'In-Progress'
            intrabatch_dupl_count = df_duplicate.agg(py_function.count('batch_id').cast(IntegerType()).alias('intrabatch_dupl_count')).rdd.flatMap(lambda row: row).collect()
            intrabatch_dist_dupl_count = df_duplicate.agg(py_function.approx_count_distinct('batch_id').cast(IntegerType()).alias('intrabatch_dist_dupl_count')).rdd.flatMap(lambda row: row).collect()
            metaQuery = ("update uk_rrbs_dm.log_batch_status_rrbs set intrabatch_dupl_count={intrabatch_dupl_count}, batch_status='{batch_status}', intrabatch_dist_dupl_count={intrabatch_dist_dupl_count} where batch_id={batch_id}"
                .format(batch_id=batchid, batch_status=batch_status, intrabatch_dupl_count=''.join(str(e) for e in intrabatch_dupl_count), intrabatch_dist_dupl_count=''.join(str(e) for e in intrabatch_dist_dupl_count)))
            self.redshiftprop.writeBatchStatus(self.sparkSession, metaQuery)
            df_unique_late = self.trans.getUnique(lateOrNormalCdr, "rec_checksum").filter("normalOrlate == 'Late'")
            intrabatch_late_count = df_unique_late.agg(py_function.count('batch_id').cast(IntegerType()).alias('intrabatch_late_count')).rdd.flatMap(lambda row: row).collect()
            metaQuery = ("update uk_rrbs_dm.log_batch_status_rrbs set intrabatch_late_count={intrabatch_late_count} where batch_id={batch_id}".format(batch_id=batchid, intrabatch_late_count=''.join(str(e) for e in intrabatch_late_count)))
            self.redshiftprop.writeBatchStatus(self.sparkSession, metaQuery)
            df_unique_normal = self.trans.getUnique(lateOrNormalCdr, "rec_checksum").filter("normalOrlate == 'Normal'")
            intrabatch_new_count = df_unique_normal.agg(py_function.count('batch_id').cast(IntegerType()).alias('intrabatch_new_count')).rdd.flatMap(lambda row: row).collect()
            intrabatch_status = 'Complete'
            metaQuery = ("update uk_rrbs_dm.log_batch_status_rrbs set intrabatch_new_count={intrabatch_new_count}, intrabatch_status='{intrabatch_status}' where batch_id={batch_id}".format(batch_id=batchid, intrabatch_status=intrabatch_status, intrabatch_new_count=''.join(str(e) for e in intrabatch_new_count)))
            self.redshiftprop.writeBatchStatus(self.sparkSession, metaQuery)
            self.logger.info("***** source data prepared for transformation *****")
            record_count = df_source.groupBy('filename').agg(py_function.count('batch_id').alias('record_count').cast(IntegerType()))
            return df_duplicate, df_unique_late, df_unique_normal, record_count
        except Exception as ex:
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
            latecdr_count = dfLateCDRNewRecord.groupBy('filename').agg(py_function.count('batch_id').alias('latecdr_dm_count').cast(IntegerType()),py_function.count('batch_id').alias('latecdr_lm_count').cast(IntegerType()))
            latecdr_dupl_count = dfLateCDRDuplicate.groupBy('filename').agg(py_function.count('batch_id').alias('latecdr_duplicate_count').cast(IntegerType()))
            ldm_latecdr_count = dfLateCDRNewRecord.agg(py_function.count('batch_id').cast(IntegerType()).alias('intrabatch_dupl_count')).rdd.flatMap(lambda row: row).collect()
            ldm_latecdr_dupl_count = dfLateCDRDuplicate.agg(py_function.count('batch_id').cast(IntegerType()).alias('intrabatch_dupl_count')).rdd.flatMap(lambda row: row).collect()
            ldm_latecdr_status = 'Complete'
            metaQuery = ("update uk_rrbs_dm.log_batch_status_rrbs set ldm_latecdr_status='{ldm_latecdr_status}', ldm_latecdr_count={ldm_latecdr_count}, ldm_latecdr_dupl_count={ldm_latecdr_dupl_count} where batch_id={batch_id}"
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
            normalcdr_count = dfNormalCDRNewRecord.groupBy('filename').agg(py_function.count('batch_id').alias('newrec_dm_count').cast(IntegerType()))
            normalcdr_dupl_count = dfNormalCDRDuplicate.groupBy('filename').agg(py_function.count('batch_id').alias('newrec_duplicate_count').cast(IntegerType()))
            dm_normal_count = dfNormalCDRNewRecord.agg(py_function.count('batch_id').cast(IntegerType()).alias('intrabatch_dupl_count')).rdd.flatMap(lambda row: row).collect()
            dm_normal_dupl_count = dfNormalCDRDuplicate.agg(py_function.count('batch_id').cast(IntegerType()).alias('intrabatch_dupl_count')).rdd.flatMap(lambda row: row).collect()
            dm_normal_status = 'Complete'
            metaQuery = ("update uk_rrbs_dm.log_batch_status_rrbs set dm_normal_status='{dm_normal_status}', dm_normal_count={dm_normal_count}, dm_normal_dupl_count={dm_normal_dupl_count} where batch_id={batch_id}"
                .format(batch_id=batchid, dm_normal_status=dm_normal_status, dm_normal_count=''.join(str(e) for e in dm_normal_count),dm_normal_dupl_count=''.join(str(e) for e in dm_normal_dupl_count)))
            self.redshiftprop.writeBatchStatus(self.sparkSession, metaQuery)
            return dfNormalCDRNewRecord, dfNormalCDRDuplicate, normalcdr_count, normalcdr_dupl_count
        except Exception as ex:
            self.logger.error("Failed to compute Normal CDR data : {error}".format(error=ex))

    def writetoDataMart(self,srcDataframe: DataFrame, batchStatusDF : (DataFrame, None), tgtColmns=[]):
        try:
            self.logger.info("***** started writing to data mart *****")
            df = srcDataframe.select(*tgtColmns)
            self.redshiftprop.writeToRedshift(df, self.property.get("database"), self.property.get("normalcdrtbl"))
            self.logger.info("***** started writing to data mart - completed *****")
            if batchStatusDF:
                batch_end_dt = datetime.now()
                batchStatusDF.withColumn('batch_end_dt', py_function.lit(batch_end_dt))
        except Exception as ex:
            self.logger.error("Failed to write data in data mart : {error}".format(error=ex))

    def writetoDuplicateCDR(self, dataframe: DataFrame, tgtColmns=[]):
        try:
            self.logger.info("***** started writing to duplicate mart *****")
            df = dataframe.select(*tgtColmns)
            self.redshiftprop.writeToRedshift(df, self.property.get("database"), self.property.get("duplicatecdrtbl"))
            self.logger.info("***** started writing to duplicate mart - completed *****")
        except Exception as ex:
            self.logger.error("Failed to write data in duplicate mart : {error}".format(error=ex))

    def writetoLateCDR(self, srcDataframe: DataFrame, tgtColmns=[]):
        try:
            self.logger.info("***** started writing to late mart *****")
            df = srcDataframe.select(*tgtColmns)
            self.redshiftprop.writeToRedshift(df, self.property.get("database"), self.property.get("latecdrtbl"))
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
