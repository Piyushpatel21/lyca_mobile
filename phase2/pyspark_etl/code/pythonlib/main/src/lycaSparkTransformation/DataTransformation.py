########################################################################
# description     : Spark Transformation, Spark writer                 #                              #
# author          : Naren K(narendra.kumar@cloudwick.com),             #
#                   Tejveer Singh(tejveer.singh@cloudwick.com)         #
#                   Shubhajit Saha(shubhajit.saha@cloudwick.com)       #
# contributor     :                                                    #
# version         : 1.0                                                #
# notes           :                                                    #
########################################################################

from commonUtils.JsonProcessor import JsonProcessor
from pyspark.sql import DataFrame, Window
from functools import reduce
from pyspark.sql import functions as py_function
from pyspark.sql.types import StructType
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql.types import IntegerType, StringType
from commonUtils.Log4j import Log4j


class DataTransformation:

    def __init__(self):
        self._logger = Log4j().getLogger()

    def readSourceFile(self, spark, path, structtype: StructType, batchid, prmryKey, checkSumColumns=[],
                       fList=[]) -> DataFrame:
        """ :parameter spark
            :parameter path of source files
            :parameter structtype - schema for source file
            :parameter checkSumColumns - list of checksum columns
            :parameter fList of source files
            :return union of all source files"""
        try:
            self._logger.info("Started reading source files")
            df_list = []
            for file in fList:
                file_identifier = str(file).lower().replace(".cdr", "")
                df_source = "df_" + file_identifier
                self._logger.info("Reading source file : {file}".format(file=file))
                file = path + file
                print(file)
                df_source = spark.read.option("header", "false").option("dateFormat", 'dd-MM-yyyy').schema(structtype).csv(file)
                df_trans = df_source.withColumn("rec_checksum",
                                                py_function.md5(py_function.concat_ws(",", *checkSumColumns))) \
                    .withColumn("filename", py_function.lit(file_identifier)) \
                    .withColumn(prmryKey, py_function.lit(1)) \
                    .withColumn("batch_id", py_function.lit(batchid).cast(IntegerType())) \
                    .withColumn("created_date", py_function.current_timestamp()) \
                    .withColumn("free_zone_expiry_date_num", py_function.lit(1).cast(IntegerType()))
                self._logger.info("Merging all source file using union all")
                df_list.append(df_trans)
            return reduce(DataFrame.union, df_list)
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

    def getDuplicates(self, dataFrame: DataFrame, checksumColumn) -> DataFrame:
        """:parameter - DataFrame, checkSum column name
           :return duplicate record with in dataFrame basis on checksum column"""
        try:
            self._logger.info("Identifying duplicate records within source ")
            windowspec = Window.partitionBy(dataFrame[checksumColumn]).orderBy(dataFrame[checksumColumn].desc())
            df_duplicates = dataFrame.withColumn("duplicate",
                                                 py_function.count(dataFrame[checksumColumn]).over(windowspec).cast(
                                                     IntegerType())) \
                .filter('duplicate > 1')
            self._logger.info("Return duplicate records")
            return df_duplicates
        except Exception as ex:
            self._logger.error("Failed to return duplicate within source: {error}".format(error=ex))

    def getUnique(self, dataFrame: DataFrame, checksumColumn) -> DataFrame:
        """:parameter - DataFrame, checkSum column name
            :return unique record with in dataFrame basis on checksum column"""
        try:
            self._logger.info("Identifying unique records within source")
            windowspec = Window.partitionBy(dataFrame[checksumColumn]).orderBy(dataFrame[checksumColumn])
            df_source = dataFrame.withColumn("duplicate", py_function.row_number().over(windowspec).cast(IntegerType()))
            df_unique_records = df_source.filter(df_source['duplicate'] == 1).drop(df_source['duplicate'])
            self._logger.info("Return unique records")
            return df_unique_records
        except Exception as ex:
            self._logger.error("Failed to return unique records : {error}".format(error=ex))

    def getPrevRangeDate(self, run_date, mnthOrdaily=None, noOfdaysOrMonth=None):
        """:parameter - monthly or daily and no. of month or days
           :return difference date between current date and given no. of days and month"""
        try:
            if mnthOrdaily == 'daily':
                d = datetime.strptime(str(run_date), '%Y%m%d') + relativedelta(days=-noOfdaysOrMonth)
                check_date = d.strftime("%Y%m%d")
                return check_date
            elif mnthOrdaily == 'monthly':
                d = datetime.strptime(str(run_date), '%Y%m%d') + relativedelta(months=-noOfdaysOrMonth)
                check_date = d.strftime("%Y%m%d")
                return check_date
            else:
                d = datetime.strptime(str(run_date), '%Y%m%d') + relativedelta(days=-1)
                check_date = d.strftime("%Y%m%d")
                return check_date
        except Exception as ex:
            self._logger.error("Failed to compute date range : {error}".format(error=ex))

    def getLateOrNormalCdr(self, dataFrame: DataFrame, dateColumn, formattedDateColumn, integerDateColumn,
                           dateRange) -> DataFrame:
        """:parameter dataFrame- source as dataFrame
           :parameter dateColumn column
           :parameter formattedDateColumn - formatted Date Column name
           :parameter integerDateColumn - numeric column name of date column
           :parameter dateRange
           :return dataframe with new derived columns"""
        try:
            self._logger.info("Identifying late and normal records within source")
            df_event_date = dataFrame.withColumn(integerDateColumn,
                                                 py_function.substring(py_function.col(dateColumn), 0, 8).cast(
                                                     IntegerType()))
            df_normalOrLate = df_event_date.withColumn(formattedDateColumn, py_function.to_date(
                py_function.to_date(py_function.col(integerDateColumn).cast(StringType()), 'yyyyMMdd'), 'yyyy-MM-dd')) \
                .withColumn("normalOrlate",
                            py_function.when(py_function.col(integerDateColumn) < int(dateRange), "Late").otherwise(
                                "Normal"))
            return df_normalOrLate
        except Exception as ex:
            self._logger.error("Failed to return late and normal records : {error}".format(error=ex))

    def checkDuplicate(self, dfSource: DataFrame, dfRedshift: DataFrame) -> DataFrame:
        """:parameter dfSource - get from source file
           :parameter dfRedshift - reading data from redshift late CDR or data mart db
           :return dataframe with new column weather record exist in dfRedshift"""
        try:
            self._logger.info("Identifying db duplicate within source")
            dfDB = dfRedshift.select(dfRedshift["rec_checksum"])
            dfjoin = dfSource.join(dfDB, dfSource["rec_checksum"] == dfDB["rec_checksum"], "left_outer") \
                .withColumn("newOrDupl",
                            py_function.when(dfSource["rec_checksum"] == dfDB["rec_checksum"], "Duplicate").otherwise(
                                "New"))
            dfnormalOrDuplicate = dfjoin.drop(dfDB["rec_checksum"])
            return dfnormalOrDuplicate
        except Exception as ex:
            self._logger.error("Failed to return unique records : {error}".format(error=ex))
