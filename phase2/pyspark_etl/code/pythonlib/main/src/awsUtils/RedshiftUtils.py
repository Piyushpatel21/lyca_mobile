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

    def getFileList(self, sparkSession: SparkSession, batchid) -> []:
        try:
            files = sparkSession.read \
                .format("com.databricks.spark.redshift") \
                .option("url", self.jdbcUrl) \
                .option("forward_spark_s3_credentials", "true") \
                .option("query",
                        "SELECT filename FROM uk_rrbs_dm.log_batch_files_rrbs where batch_id = {batch_id}".format(
                            batch_id=batchid)) \
                .option("tempdir", self.redshiftTmpDir) \
                .load()
            filename = files.rdd.flatMap(lambda file: file).collect()
            return filename
        except Exception as ex:
            self._logger.error("failed to get file list from redshift : {error}".format(error=ex))

    def getBatchId(self, sparkSession: SparkSession, source_identifier, prevDate) -> int:
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
            query = "SELECT DISTINCT batch_id FROM uk_rrbs_dm.log_batch_files_rrbs WHERE file_source LIKE '%{source_identifier}%' AND batch_from <= '{prevDate}' AND batch_to => '{prevDate}'".format(
                source_identifier=source_identifier, prevDate=prevDate)
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

    def writeBatchFileStatus(self, sparkSession: SparkSession, dataframe: DataFrame, batchId):
        """
        Update log_batch_files_rrbs table in Redshift with file record count and batch status
        :parameter sparkSession - spark session
        :parameter dataframe - dataframe with filename, record_count and batch_status
        :parameter batch_id - batch id for the batch interval to read files
        :return:
        """
        global joinedDF
        try:
            self._logger.info("Updating Log Batch Files RRBS table :")
            query = "SELECT batch_id, file_source, file_id, filename, batch_from, batch_to, is_valid, batch_createtime FROM uk_rrbs_dm.log_batch_files_rrbs WHERE batch_id ='{batchId}'".format(
                batchId=batchId)
            self._logger.info("Query {query}".format(query=query))
            df = sparkSession.read \
                .format("com.databricks.spark.redshift") \
                .option("url", self.jdbcUrl) \
                .option("query", query) \
                .option("forward_spark_s3_credentials", "true") \
                .option("tempdir", self.redshiftTmpDir) \
                .load()
            df2 = df.withColumnRenamed("filename", "file_name")
            joinedDF = df2.join(dataframe, df2.file_name == dataframe.filename, "left_outer")
        except Exception as ex:
            self._logger.error("failed to read log_batch_status data from redshift : {error}".format(error=ex))
        arrangedDF = joinedDF.select("batch_id", "file_source", "file_id", "filename", "batch_from", "batch_to",
                                     "record_count", "newrec_dm_count", "latecdr_dm_count", "latecdr_lm_count",
                                     "newrec_duplicate_count",
                                     "latecdr_duplicate_count", "is_valid", "batch_createtime")

        preDelQuery = "DELETE FROM uk_rrbs_dm.log_batch_files_rrbs WHERE batch_id='{batchId}'".format(batchId=batchId)
        table = "uk_rrbs_dm.log_batch_files_rrbs"
        try:
            arrangedDF.write.format("com.databricks.spark.redshift") \
                .option("url", self.jdbcUrl) \
                .option("dbtable", table) \
                .option("preactions", preDelQuery) \
                .option("forward_spark_s3_credentials", "true") \
                .option("tempdir", self.redshiftTmpDir) \
                .mode("append") \
                .save()
            self._logger.info("Successfully Updated Log Batch Files table :")
        except Exception as ex:
            self._logger.error("failed to write log_batch_status data to redshift : {error}".format(error=ex))

    def writeBatchStatus(self, sparkSession: SparkSession, query):
        """
        Update log_batch_status_rrbs table in Redshift with file record count and batch status
        :parameter sparkSession - spark session
        :parameter dataframe - dataframe with filename, record_count and batch_status
        :parameter batch_id - batch id for the batch interval to read files
        :return:
        """

        schema = StructType(
            [StructField('batch_id', IntegerType(), True),
             StructField('s3_batchreadcount', IntegerType(), True),
             StructField('s3_filecount', IntegerType(), True),
             StructField('intrabatch_new_count', IntegerType(), True),
             StructField('intrabatch_late_count', IntegerType(), True),
             StructField('intrabatch_dupl_count', IntegerType(), True),
             StructField('intrabatch_dist_dupl_count', IntegerType(), True),
             StructField('intrabatch_status', StringType(), True),
             StructField('dm_normal_count', IntegerType(), True),
             StructField('dm_normal_dupl_count', IntegerType(), True),
             StructField('dm_normal_status', StringType(), True),
             StructField('ldm_latecdr_count', IntegerType(), True),
             StructField('ldm_latecdr_dupl_count', IntegerType(), True),
             StructField('ldm_latecdr_status', StringType(), True),
             StructField('batch_start_dt', StringType(), True),
             StructField('batch_end_dt', StringType(), True),
             StructField('batch_status', StringType(), True)])

        data = [(0, 0, 0, 0, 0, 0, 0, '', 0, 0, '', 0, 0, '', '2020-05-08 07:14:50', '2020-05-08 07:14:50', '')]
        rdd = sparkSession.sparkContext.parallelize(data)
        df = sparkSession.createDataFrame(rdd, schema)
        preQuery = query
        postQuery = "DELETE FROM uk_rrbs_dm.log_batch_status_rrbs WHERE batch_id = 0"
        table = "uk_rrbs_dm.log_batch_status_rrbs"
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
