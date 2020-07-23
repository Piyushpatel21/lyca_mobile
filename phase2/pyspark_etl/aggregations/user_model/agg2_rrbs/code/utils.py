"""
Utils for agg1_rrbs_sms, it contains
* Redshift utility
* Log4j utility
"""

from parent_utils.redshift_utils import RedshiftUtils
from parent_utils.log4j import Log4j
from parent_utils.json_processor import JsonProcessor
from parent_utils.aws_reader import AwsReader
from parent_utils.schema_reader import SchemaReader
from parent_utils.spark_session_builder import SparkSessionBuilder
from parent_utils.secret_manager import get_secret
import datetime


class Agg2RedshiftUtils(RedshiftUtils):
    """
    Inherit class of Redshiftutils
    """

    def __init__(self, spark, dwh_host, dwh_port, dwh_db, dwh_user, dwh_pass, tmp_dir):
        """
        Initialize RedshiftUtils class
        """

        RedshiftUtils.__init__(self, spark, dwh_host, dwh_port, dwh_db, dwh_user, dwh_pass, tmp_dir)

    def write_to_redshift_with_temp_table(self, temp_table, final_table, cols, df, pre_query=None, suffix_post_query=None):
        """
        Writes the data to final table using temp table

        :param temp_table: Temp table name with format as database.table
        :param final_table: Final table name with format as database.table
        :param cols: List of columns to write
        :param df: Dataframe to be written
        :param pre_query: Pre query to run before inserting to final table
        """

        try:
            self._logger.info("Writing to {table} table using {temp_table} table."
                              .format(table=final_table,
                                      temp_table=temp_table))
            insert_statement = "INSERT INTO {final_table}({cols}) SELECT {cols} FROM {temp_table};".format(
                final_table=final_table,
                temp_table=temp_table,
                cols=','.join(cols)
            )
            post_query = insert_statement + suffix_post_query
            self._logger.info("Pre query: {query}".format(query=pre_query))
            self._logger.info("Post query: {query}".format(query=post_query))
            df.write.format("com.databricks.spark.redshift") \
                .option("url", self.jdbc_url) \
                .option("dbtable", temp_table) \
                .option("postactions", post_query) \
                .option("forward_spark_s3_credentials", "true") \
                .option("tempdir", self.redshift_tmp_dir) \
                .mode("overwrite") \
                .save()
        except Exception as ex:
            self._logger.error("Error occurred while writing to redshift with error {err}.".format(err=ex))
            raise Exception(ex)


class Agg2Log4j(Log4j):
    """
    Inherits Log4j class
    """

    def __init__(self, spark):
        """
        Initialize Log4j class
        """

        Log4j.__init__(self, spark)


class Agg2JsonProcessor(JsonProcessor):
    """
    Inherits JsonProcessor class
    """

    def __init__(self):
        """
        Initialize JsonProcessor class
        """
        JsonProcessor.__init__(self)

    @staticmethod
    def process_app_properties(module, file_content):
        """
        :parameter module - read property for a particular module
        :parameter file_content - file content of JSON
        :return JSON object
        """

        try:
            data = JsonProcessor.json_parser(file_content)
            for obj in data:
                if obj["module"] == module:
                    return obj
                else:
                    continue
        except (OSError, IOError, ValueError) as ex:
            print("Failed to process application prop file : Path - " + ex)
            raise Exception(ex)


class Agg2AwsReader(AwsReader):
    """
    Inherits AwsReader class
    """

    def __init__(self):
        """
        Initialize AwsReader class
        """

        AwsReader.__init__(self)


class Agg2SchemaReader(SchemaReader):
    """
    Inherits SchemaReader class
    """

    def __init__(self):
        """
        Initialize SchemaReader class
        """

        SchemaReader.__init__(self)


class Agg2SparkSession(SparkSessionBuilder):
    """
    Inherits GlueSpark class
    """

    def __init__(self, master=None, appname=None):
        """
        Initialize GlueSpark class
        """

        SparkSessionBuilder.__init__(self, master=master, appname=appname)


def parse_date(text):
    for fmt in ('%Y%m%d', '%Y%m', '%Y'):
        try:
            value = datetime.datetime.strptime(text, fmt)
            return fmt, value
        except ValueError:
            pass
    raise ValueError('no valid date format found')