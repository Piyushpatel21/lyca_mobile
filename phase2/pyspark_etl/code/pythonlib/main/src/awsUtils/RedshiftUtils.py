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
        self.jdbcUsername = dwh_user
        self.jdbcPassword = dwh_pass
        self.jdbcHostname = dwh_host
        self.jdbcPort = dwh_port
        self.jdbcDatabase = dwh_db
        self.redshiftTmpDir = tmp_dir
        self.jdbcUrl = "jdbc:redshift://{jdbcHostname}:{jdbcPort}/{jdbcDatabase}?user={jdbcUsername}&password={jdbcPassword}" \
            .format(jdbcHostname=self.jdbcHostname, jdbcPort=self.jdbcPort, jdbcDatabase=self.jdbcDatabase, jdbcUsername=self.jdbcUsername,
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
            print("failed to get data from redshift")

    def writeToRedshift(self, dataframe: DataFrame, db_name, dataset_name, idn):
        """
        Return response with data from Redshift
        :parameter dataframe - need to write data in redshift
        :parameter db_name - schema name
        :parameter dataset_name - table name
        :return:
        """
        try:
            cnt = dataframe.count()
            dataframe.show(20, False)
            table = ".".join([db_name, dataset_name])
            print("writing to redshift idn={idn}: cnt={cnt}, table={table}".format(idn=idn, cnt=cnt, table=table))
            dataframe.write \
                .format("com.databricks.spark.redshift") \
                .option("url", self.jdbcUrl) \
                .option("dbtable", table) \
                .option("forward_spark_s3_credentials", "true") \
                .option("tempdir", self.redshiftTmpDir) \
                .mode("append") \
                .save()
        except Exception as ex:
            print("failed to write data in redshift : {error}".format(error=ex))

    def getFileList(self, sparkSession: SparkSession, batchid) -> []:
        try:
            files = sparkSession.read \
                .format("com.databricks.spark.redshift") \
                .option("url", self.jdbcUrl) \
                .option("forward_spark_s3_credentials", "true") \
                .option("query", "SELECT file_name FROM uk_rrbs_dm.log_batch_files_rrbs where batch_id = {batch_id}".format(batch_id=batchid)) \
                .option("tempdir", self.redshiftTmpDir) \
                .load()
            filename = files.rdd.flatMap(lambda file: file).collect()
            return filename
        except Exception as ex:
            print("failed to get file list from redshift : {error}".format(error=ex))

    def getBatchId(self, sparkSession: SparkSession, source_identifier, batch_from, batch_to) -> DataFrame:
        """
        Return response with batchId from Redshift
        :parameter sparkSession - spark session
        :parameter source_identifier - source identifier(SMS,TOPUP etc.)
        :parameter batch_from - batch interval start datetime
        :parameter batch_to - batch interval end datetime
        :return:
        """
        try:
            return sparkSession.read \
                .format("com.databricks.spark.redshift") \
                .option("url", self.jdbcUrl) \
                .option("query",
                        "SELECT DISTINCT batch_id FROM uk_rrbs_dm.log_batch_files_rrbs \
                        WHERE file_source LIKE '%{source_identifier}%' \
                        AND batch_from >= '{batch_from}' AND batch_to <= '{batch_to}}'"
                        .format(source_identifier=source_identifier, batch_from=batch_from, batch_to=batch_to)) \
                .option("forward_spark_s3_credentials", "true") \
                .option("tempdir", self.redshiftTmpDir) \
                .load()
        except Exception as ex:
            print("failed to get batch Id from redshift : {error}".format(error=ex))
