########################################################################
# description     : Spark Transformation, Spark writer                 #                              #
# author          : Naren K(narendra.kumar@cloudwick.com),             #
#                   Tejveer Singh(tejveer.singh@cloudwick.com)         #
#                   Shubhajit Saha(shubhajit.saha@cloudwick.com)       #
# contributor     :                                                    #
# version         : 1.0                                                #
# notes           :                                                    #
########################################################################

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StringType, DoubleType, LongType, FloatType, DateType, TimestampType, \
    DecimalType, StructType, StructField
from commonUtils.JsonProcessor import JsonProcessor
from commonUtils.Log4j import Log4j


@udf
def getSourceFilePath(path):
    file = path.split("/")[-1]
    return path.replace(file, '')


schema = StructType(
    [StructField("source_file_name", StringType(), True),
     StructField("source_rec_count", StringType(), True)]
)


class DataTransformation:

    def __init__(self):
        self._logger = Log4j().getLogger()
        self.default_value_dict = {'string': '0', 'number': 0, 'date': '1970-01-01', 'datetime': '1970-01-01 00:00:00'}

    def readSourceFile(self, spark, batchid, encoding, fList=[]) -> DataFrame:
        """ :parameter spark
            :parameter path of source files
            :parameter structtype - schema for source file
            :parameter checkSumColumns - list of checksum columns
            :parameter fList of source files
            :return union of all source files"""
        try:
            self._logger.info("Started reading source files")
            src_schema_string = []
            # full_path_list = [path + file for file in fList]
            full_path_list = fList

            self._logger.info("Reading from file list: {list}".format(list=full_path_list))

            df_source_all = spark.read.option("header", "true").schema(schema).option("encoding", encoding).csv(full_path_list)

            df_source = df_source_all.withColumn("recon_file_name", F.input_file_name()) \
                .withColumn("recon_file_name", F.reverse(F.split('recon_file_name', '/'))[0]) \
                .withColumn("s3_prefix", F.lit(getSourceFilePath(F.input_file_name())))

            df_trimmed = self.trimAllCols(df_source)
            df_cleaned_checksum = self.cleanDataForChecksum(df_trimmed)
            df_trans = df_cleaned_checksum \
                .withColumn("batch_id", F.lit(batchid).cast(IntegerType())) \
                .withColumn("created_date", F.current_timestamp())
            return df_trans
        except Exception as ex:
            self._logger.error("Failed to merge all source files with error: {error}".format(error=ex))

    def getCheckSumColumns(self, JsonPath) -> []:
        """ :parameter JsonPath schema file path
            :return list of column which is part of building checksum column"""
        try:
            self._logger.info("Reading source json to get check sum column list")
            data = JsonProcessor.json_parser(JsonPath)
            checkColList = []
            for col in data:
                if col["check_sum"]:
                    checkColList.append(col["column_name"])
            self._logger.info("Retrun checksum column list to compute md5")
            return checkColList
        except Exception as ex:
            self._logger.error("Failed to return check sum columns list: {error}".format(error=ex))

    def getTgtColumns(self, JsonPath) -> []:
        """ :parameter JsonPath schema file path
            :return list of column which is part of building checksum column"""
        try:
            self._logger.info("Reading tgt json to get tgt schema column list")
            data = JsonProcessor.json_parser(JsonPath)
            colList = []
            for col in data:
                colList.append(col["column_name"])
            self._logger.info("Return tgt schema for writing in target table")
            return colList
        except Exception as ex:
            self._logger.error("Failed to return check sum columns list: {error}".format(error=ex))

    def trimColumn(self, column):
        """
        Trims the white space from start and end

        :param column:
        :return:
        """
        return F.trim(column)

    def fillNull(self, df, value_dict=None):
        """
        Fill the null value with respect to data type.

        :param df: spark dataframe
        :param value_dict: default: {'string': '0', 'number': 0, 'date': '1970-01-01', 'datetime': '1970-01-01 00:00:00'}
        :return:
        """

        if not value_dict:
            value_dict = self.default_value_dict

        col_default_values = {}
        for elem in df.schema:
            if elem.dataType == StringType():
                col_default_values[elem.name] = value_dict['string']
            elif elem.dataType in [IntegerType(), DoubleType(), FloatType(), LongType()]:
                col_default_values[elem.name] = value_dict['number']
            elif elem.dataType == DateType():
                col_default_values[elem.name] = value_dict['date']
            elif elem.dataType == TimestampType():
                col_default_values[elem.name] = value_dict['datetime']

        return df.fillna(col_default_values)

    def fillBlanks(self, df, value=None):
        """
        Fill the blank column with the value specified.

        :param df:
        :param value:
        :return:
        """
        final_df = df
        for elem in df.schema:
            if elem.dataType == StringType():
                final_df = final_df.withColumn(elem.name,
                                               F.when(F.col(elem.name) == "", value)
                                               .otherwise(
                                                   F.when(F.col(elem.name) == " ", value).otherwise(F.col(elem.name))))
        return final_df

    def trimAllCols(self, df):
        """
        Trim all the space string from columns

        :param df: spark dataframe
        :return:
        """
        final_df = df
        for elem in df.schema:
            if elem.dataType == StringType():
                final_df = final_df.withColumn(elem.name, self.trimColumn(F.col(elem.name)))

        return final_df

    def cleanDataForChecksum(self, df):
        """
        Clean the data for generating checksum

        :param df:
        :return:
        """
        new_df = df
        for elem in new_df.schema:
            if elem.dataType != StringType():
                new_df = new_df.withColumn(elem.name, new_df[elem.name].cast(StringType()))
        no_blanks_df = self.fillBlanks(new_df, "0")
        no_null_df = self.fillNull(no_blanks_df)
        return no_null_df
