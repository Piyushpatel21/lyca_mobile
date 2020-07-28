"""
Contains functions for interacting with Redshift with following functionalities:
* Read from Redshift
* Write to Redshift
"""

from parent_utils.log4j import Log4j


class RedshiftUtils:

    def __init__(self, spark, dwh_host, dwh_port, dwh_db, dwh_user, dwh_pass, tmp_dir):
        """
            Initialize class with required parameters for connecting to data warehouse.
            :param dwh_host:  Hostname for DWH
            :param dwh_port: Port for DWH
            :param dwh_db: Database name to connect. ex. cdap
            :param dwh_user: Username to use for connection
            :param dwh_pass: Password for the user
            :param tmp_dir:  Temp directory for store intermediate result
            """
        self.spark = spark
        self._logger = Log4j(self.spark).getLogger()
        self.jdbc_username = dwh_user
        self.jdbc_password = dwh_pass
        self.jdbc_hostname = dwh_host
        self.jdbc_port = dwh_port
        self.jdbc_database = dwh_db
        self.redshift_tmp_dir = tmp_dir
        self.jdbc_url = "jdbc:redshift://{jdbcHostname}:{jdbcPort}/{jdbcDatabase}?user={jdbcUsername}&password={jdbcPassword}" \
            .format(jdbcHostname=self.jdbc_hostname, jdbcPort=self.jdbc_port, jdbcDatabase=self.jdbc_database,
                    jdbcUsername=self.jdbc_username,
                    jdbcPassword=self.jdbc_password)

    def read_from_redshift_with_query(self, query):

        try:
            self._logger.info("Reading from redshift {url} with query {query}.".format(
                url=self.jdbc_hostname, query=query
            ))
            # Read data from a query
            df = self.spark.read \
                .format("com.databricks.spark.redshift") \
                .option("url", self.jdbc_url) \
                .option("query", query) \
                .option("forward_spark_s3_credentials", "true") \
                .option("tempdir", self.redshift_tmp_dir) \
                .load()
            return df
        except Exception as ex:
            self._logger.error("Failed to read from redshift with error {err}.".format(err=ex))
            raise Exception(ex)

    def write_to_redshift_with_temp_table(self, temp_table, final_table, cols, df, pre_query=None, suffix_post_query=None, truncate_temp=True):
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

            if suffix_post_query:
                post_query = insert_statement + suffix_post_query
            else:
                post_query = insert_statement

            if truncate_temp:
                if post_query.endswith(';'):
                    truncate_temp_query = "TRUNCATE TABLE {temp_table};".format(temp_table=temp_table)
                else:
                    truncate_temp_query = ";TRUNCATE TABLE {temp_table};".format(temp_table=temp_table)
                post_query = post_query + truncate_temp_query

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
