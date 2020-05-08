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
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker


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

        self.url = "jdbc:redshift://{host}:{port}/{db}".format(host=dwh_host, port=dwh_port, db=dwh_db)
        self.user = dwh_user,
        self.password = dwh_pass,
        self.redshiftTmpDir = tmp_dir
        self.connection_string = "redshift+psycopg2://%s:%s@%s:%s/%s" % (self.user, self.password, dwh_host, str(dwh_port), dwh_db)

    def readFromRedshift(self, sparkSession: SparkSession, domain_name, dataset_name) -> DataFrame:
        """
        Return response with data from Redshift
        :parameter sparkSession - spark session
        :parameter domain_name - schema name
        :parameter dataset_name - table name
        :return:
        """
        try:

            table = ".".join([domain_name, dataset_name])
            return sparkSession.read \
                .format("com.databricks.spark.redshift") \
                .option("url", self.url) \
                .option("dbtable", table) \
                .option("tempdir", self.redshiftTmpDir) \
                .load()

        except Exception as ex:
            print("failed to get data from redshift")

    def writeToRedshift(self, dataframe, domain_name, dataset_name):
        """
        Return response with data from Redshift
        :parameter dataframe - need to write data in redshift
        :parameter domain_name - schema name
        :parameter dataset_name - table name
        :return:
        """
        try:
            table = ".".join([domain_name, dataset_name])
            dataframe.write \
                .format("com.databricks.spark.redshift") \
                .option("url", self.url) \
                .option("dbtable", table) \
                .option("tempdir", self.redshiftTmpDir) \
                .mode("append") \
                .save()
        except Exception as ex:
            print("failed to write data in redshift")

    def getFileList(self, batchid) -> []:
        filename = []
        engine = sa.create_engine(self.connection_string)
        session = sessionmaker()
        session.configure(bind=engine)
        s = session()
        query = "SELECT file_name FROM uk_test.log_batch_files_rrbs where batch_id = 1};"
        rr = s.execute(query)
        records = rr.fetchall()
        for row in records:
            filename.append(row)
