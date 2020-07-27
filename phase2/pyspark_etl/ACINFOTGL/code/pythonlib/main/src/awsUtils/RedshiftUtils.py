########################################################################
# description     : processing JSON config files.                      #
# author          : Naren K(narendra.kumar@cloudwick.com),             #
#                   Tejveer Singh(tejveer.singh@cloudwick.com)         #
#                   Shubhajit Saha(shubhajit.saha@cloudwick.com)       #
# contributor     :                                                    #
# version         : 1.0                                                #
# notes           :                                                    #
########################################################################

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import *

from commonUtils.Log4j import Log4j


class RedshiftUtils:

    def __init__(self, dwh_host, dwh_port, dwh_db, dwh_user, dwh_pass, tmp_dir):
        """
            Initialize class with required parameters for connecting to data warehouse.
            :param dwh_type: Is it "redshift" or "aurora"
            :param dwh_host:  Hostname for DWH
            :param dwh_port: Port for DWH
            :param dwh_db: Database name to connect. ex. cdap
            :param dwh_user: Username to use for connection
            :param dwh_pass: Password for the user
            :param tmp_dir:  Temp directory for store intermediate result
            """
        self._logger = Log4j().getLogger()
        self.jdbcUsername = dwh_user
        self.jdbcPassword = dwh_pass
        self.jdbcHostname = dwh_host
        self.jdbcPort = dwh_port
        self.jdbcDatabase = dwh_db
        self.redshiftTmpDir = tmp_dir
        self.jdbcUrl = "jdbc:redshift://{jdbcHostname}:{jdbcPort}/{jdbcDatabase}?user={jdbcUsername}&password={jdbcPassword}" \
            .format(jdbcHostname=self.jdbcHostname, jdbcPort=self.jdbcPort, jdbcDatabase=self.jdbcDatabase,
                    jdbcUsername=self.jdbcUsername,
                    jdbcPassword=self.jdbcPassword)

    def readFromRedshift(self, sparkSession: SparkSession, db_name, dataset_name) -> DataFrame:
        """
        Return response with data from Redshift
        :parameter sparkSession - spark session
        :parameter db_name - schema name
        :parameter dataset_name - table name
        :return:
        """
        try:

            table = ".".join([db_name, dataset_name])
            return sparkSession.read \
                .format("com.databricks.spark.redshift") \
                .option("url", self.jdbcUrl) \
                .option("dbtable", table) \
                .option("forward_spark_s3_credentials", "true") \
                .option("tempdir", self.redshiftTmpDir) \
                .load()

        except Exception as ex:
            self._logger.error("failed to get data from redshift : {error}".format(error=ex))

    def writeToRedshift(self, df: DataFrame, db_name, dataset_name, tgtSchemaCols=[]):
        """
        Return response with data from Redshift
        :parameter df - need to write data in redshift
        :parameter db_name - schema name
        :parameter dataset_name - table name
        :return:
        """
        cols = str((*tgtSchemaCols,)).replace("'", "")
        table = ".".join([db_name, dataset_name])
        tempTable = ".temp_".join([db_name, dataset_name])
        post_Query_1 = "INSERT INTO {table} {cols}".format(table=table, cols=cols)
        post_Query_2 = " select {cols} from {tempTable}".format(tempTable=tempTable, cols=cols)
        postQuery = post_Query_1 + post_Query_2.replace("(", "").replace(")", "")
        try:
            df.write.format("com.databricks.spark.redshift") \
                .option("url", self.jdbcUrl) \
                .option("dbtable", tempTable) \
                .option("postactions", postQuery) \
                .option("forward_spark_s3_credentials", "true") \
                .option("tempdir", self.redshiftTmpDir) \
                .mode("overwrite") \
                .save()
        except Exception as ex:
            self._logger.error("failed to write data in redshift : {error}".format(error=ex))

    def getFileList(self, sparkSession: SparkSession, logBatchFileTbl, batchid) -> []:
        try:
            _abs_path = []
            files = sparkSession.read \
                .format("com.databricks.spark.redshift") \
                .option("url", self.jdbcUrl) \
                .option("forward_spark_s3_credentials", "true") \
                .option("query",
                        "SELECT file_name,target_system FROM {log_batch_files} where batch_id = {batch_id}"
                        .format(log_batch_files=logBatchFileTbl, batch_id=batchid)) \
                .option("tempdir", self.redshiftTmpDir) \
                .load()
            for col_files in files.rdd.collect():
                _path = str(col_files.target_system) + '/' + str(col_files.file_name)
                _abs_path.append(_path)
            return _abs_path

        except Exception as ex:
            self._logger.error("failed to get file list from redshift : {error}".format(error=ex))

    def getBatchId(self, sparkSession: SparkSession, logBatchFileTbl, source_identifier, prevDate) -> int:
        """
        Return response with batchId from Redshift
        :parameter sparkSession - spark session
        :parameter source_identifier - source identifier(SMS,TOPUP etc.)
        :parameter prevDate - previous date to run the batch
        :return:
        """
        try:
            self._logger.info(
                "Batch ID info : source_identifier={source_identifier}, previousDate={prevDate}".format(
                    prevDate=prevDate, source_identifier=source_identifier))
            query = "SELECT DISTINCT batch_id FROM {log_batch_files} WHERE file_source LIKE '%{source_identifier}%' AND batch_from <= '{prevDate}' AND batch_to >= '{prevDate}'"\
                .format(log_batch_files=logBatchFileTbl, source_identifier=source_identifier, prevDate=prevDate)
            self._logger.info("Query {query}".format(query=query))
            df = sparkSession.read \
                .format("com.databricks.spark.redshift") \
                .option("url", self.jdbcUrl) \
                .option("query", query) \
                .option("forward_spark_s3_credentials", "true") \
                .option("tempdir", self.redshiftTmpDir) \
                .load()
            batchIDList = df.rdd.flatMap(lambda batch: batch).collect()
            batchID = batchIDList[0]
            return batchID
        except Exception as ex:
            self._logger.error("failed to get batch Id from redshift : {error}".format(error=ex))

    def writeBatchFileStatus(self, sparkSession: SparkSession, logBatchFileTbl, metaDF: DataFrame, batchId):
        """
        Update log_batch_files table in Redshift with file record count and batch status
        :parameter sparkSession - spark session
        :parameter dataframe - dataframe with filename, record_count and batch_status
        :parameter batch_id - batch id for the batch interval to read files
        :return:
        """
        def getMetadataDF() -> DataFrame:
            try:
                self._logger.info("Updating Log Batch Files table :")
                query = "SELECT * FROM {log_batch_files} WHERE batch_id ='{batchId}'" \
                    .format(log_batch_files=logBatchFileTbl, batchId=batchId)
                self._logger.info("Query {query}".format(query=query))
                redshiftDF = sparkSession.read \
                    .format("com.databricks.spark.redshift") \
                    .option("url", self.jdbcUrl) \
                    .option("query", query) \
                    .option("forward_spark_s3_credentials", "true") \
                    .option("tempdir", self.redshiftTmpDir) \
                    .load()
                df = redshiftDF.join(metaDF, on='FILE_NAME', how='left')
                return df.select(redshiftDF['BATCH_ID'], redshiftDF['FILE_SOURCE'], redshiftDF['TARGET_SYSTEM'], redshiftDF['FILE_ID'], redshiftDF['FILE_NAME'], redshiftDF['BATCH_FROM'],
                                 redshiftDF['BATCH_TO'], metaDF['RECORD_COUNT'], metaDF['DM_NORMAL_COUNT'], redshiftDF['DM_LATECDR_COUNT'],
                                 redshiftDF['LDM_LATECDR_COUNT'], metaDF['DM_NORMAL_DBDUPL_COUNT'],
                                 redshiftDF['DM_LATECDR_DBDUPL_COUNT'], redshiftDF['IS_VALID'],
                                 redshiftDF['BATCH_CREATETIME'])

            except Exception as ex:
                self._logger.error("failed to read log_batch_files_status data from redshift : {error}".format(error=ex))
        batchFileDF = getMetadataDF()
        preDelQuery = "DELETE FROM {log_batch_files} WHERE batch_id='{batchId}'".format(log_batch_files=logBatchFileTbl,batchId=batchId)
        table = logBatchFileTbl
        try:
            batchFileDF.write.format("com.databricks.spark.redshift") \
                .option("url", self.jdbcUrl) \
                .option("dbtable", table) \
                .option("preactions", preDelQuery) \
                .option("forward_spark_s3_credentials", "true") \
                .option("tempdir", self.redshiftTmpDir) \
                .mode("append") \
                .save()
            self._logger.info("Successfully Updated Log Batch Files table :")
        except Exception as ex:
            self._logger.error("failed to write log_batch_files_status data to redshift : {error}".format(error=ex))

    def writeBatchStatus(self, sparkSession: SparkSession, logBatchStatusTbl, query):
        """
        Update log_batch_status table in Redshift with file record count and batch status
        :parameter sparkSession - spark session
        :parameter dataframe - dataframe with filename, record_count and batch_status
        :parameter batch_id - batch id for the batch interval to read files
        :return:
        """

        schema = StructType(
            [StructField('BATCH_ID', IntegerType(), True),
             StructField('S3_BATCHREADCOUNT', IntegerType(), True),
             StructField('S3_FILECOUNT', IntegerType(), True),
             StructField('INTRABATCH_NEW_NORMAL_COUNT', IntegerType(), True),
             StructField('INTRABATCH_NEW_LATE_COUNT', IntegerType(), True),
             StructField('INTRABATCH_DUPL_COUNT', IntegerType(), True),
             StructField('INTRABATCH_DIST_DUPL_COUNT', IntegerType(), True),
             StructField('INTRABATCH_DEDUPL_STATUS', StringType(), True),
             StructField('DM_NORMAL_COUNT', IntegerType(), True),
             StructField('DM_NORMAL_DBDUPL_COUNT', IntegerType(), True),
             StructField('DM_NORMAL_STATUS', StringType(), True),
             StructField('LDM_LATECDR_COUNT', IntegerType(), True),
             StructField('LDM_LATECDR_DBDUPL_COUNT', IntegerType(), True),
             StructField('LDM_LATECDR_STATUS', StringType(), True),
             StructField('BATCH_START_DT', StringType(), True),
             StructField('BATCH_END_DT', StringType(), True),
             StructField('BATCH_STATUS', StringType(), True)])

        data = [(0, 0, 0, 0, 0, 0, 0, '', 0, 0, '', 0, 0, '', '2020-05-08 07:14:50', '2020-05-08 07:14:50', '')]
        rdd = sparkSession.sparkContext.parallelize(data)
        df = sparkSession.createDataFrame(rdd, schema)
        preQuery = query
        postQuery = "DELETE FROM {log_batch_status} WHERE batch_id = 0".format(log_batch_status=logBatchStatusTbl)
        table = logBatchStatusTbl
        try:
            df.write.format("com.databricks.spark.redshift") \
                .option("url", self.jdbcUrl) \
                .option("dbtable", table) \
                .option("preactions", preQuery) \
                .option("postactions", postQuery) \
                .option("forward_spark_s3_credentials", "true") \
                .option("tempdir", self.redshiftTmpDir) \
                .mode("append") \
                .save()
            self._logger.info("Successfully Updated Log Batch Status table :")
        except Exception as ex:
            self._logger.error("failed to write log_batch_status data to redshift : {error}".format(error=ex))
