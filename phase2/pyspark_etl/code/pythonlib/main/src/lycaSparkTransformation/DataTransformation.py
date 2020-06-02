########################################################################
# description     : Spark Transformation, Spark writer                 #                              #
# author          : Naren K(narendra.kumar@cloudwick.com),             #
#                   Tejveer Singh(tejveer.singh@cloudwick.com)         #
#                   Shubhajit Saha(shubhajit.saha@cloudwick.com)       #
# contributor     :                                                    #
# version         : 1.0                                                #
# notes           :                                                    #
########################################################################

from datetime import datetime
from functools import reduce
from dateutil.relativedelta import relativedelta
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, DoubleType, LongType, FloatType, DateType, TimestampType, \
    DecimalType
from pyspark.sql.types import StructType, StructField

from commonUtils.JsonProcessor import JsonProcessor
from commonUtils.Log4j import Log4j


class SmsDataTransformation:
    """
    Class to perform transformations on SMS data
    """

    def __init__(self):
        self._logger = Log4j().getLogger()
        self._cdr_date_col = "msg_date"
        self._date_cols = ["free_zone_expiry_date"]
        self._date_cols_format = "dd-MM-yyyy"
        self.free_zone_expiry_date = "free_zone_expiry_date"
        self._cdr_date_col_format = "yyyyMMddHHmmss"
        self.time_zone = "Europe/London"

    def generateDerivedColumnsForSms(self, df):
        """
        Module to generate derived columns from dataframe

        :param df:
        :return:
        """

        try:
            self._logger.info("Generating derived columns for SMS data.")
            new_df = df.withColumn("_temp_datetime_col",
                                   F.to_timestamp(df[self._cdr_date_col], self._cdr_date_col_format)) \
                .withColumn("msg_date_month", F.date_format(F.col("_temp_datetime_col"), "yyyyMM").cast(IntegerType())) \
                .withColumn("msg_date_dt", F.to_date(F.col("_temp_datetime_col"))) \
                .withColumn("msg_date_num", F.date_format(F.col("_temp_datetime_col"), "yyyyMMdd").cast(IntegerType())) \
                .withColumn("msg_date_hour",
                            F.date_format(F.col("_temp_datetime_col"), "yyyyMMddHH").cast(IntegerType())) \
                .withColumn("_temp_datetime_col_utc",
                            F.to_utc_timestamp(F.col("_temp_datetime_col"), F.lit(self.time_zone))) \
                .withColumn("msg_date_time_gmt", F.from_utc_timestamp(F.col("_temp_datetime_col_utc"), "GMT")) \
                .withColumn("msg_date_month_gmt",
                            F.date_format(F.col("msg_date_time_gmt"), "yyyyMM").cast(IntegerType())) \
                .withColumn("msg_date_num_gmt",
                            F.date_format(F.col("msg_date_time_gmt"), "yyyyMMdd").cast(IntegerType())) \
                .withColumn("msg_date_hour_gmt",
                            F.date_format(F.col("msg_date_time_gmt"), "yyyyMMddHH").cast(IntegerType())) \
                .withColumn("free_zone_expiry_date_num",
                            F.date_format(df[self.free_zone_expiry_date], "yyyyMMdd").cast(IntegerType())) \
                .drop('_temp_datetime_col', '_temp_datetime_col_utc')
            return new_df
        except Exception as ex:
            self._logger.error("Failed to generate derived columns with error: {err}".format(err=ex))

    def convertTargetDataType(self, df: DataFrame, schema: StructType):
        """
        Module to convert Data Type to required format

        :param df: spark dataframe
        :param schema: schema as StructType
        :return:
        """
        # Drop whole file if error occur in converting
        new_df = df
        self._logger.info("Converting data type to required format")

        files_to_ignore = []
        for elem in schema:
            if elem.name == self._cdr_date_col:
                new_df = new_df.withColumn(elem.name, F.to_timestamp(new_df[elem.name], self._cdr_date_col_format))
            elif elem.name in self._date_cols:
                new_df = new_df.withColumn(elem.name, F.to_date(new_df[elem.name], self._date_cols_format))
            else:
                new_df = new_df.withColumn(elem.name, new_df[elem.name].cast(elem.dataType))
        return new_df


class VoiceDataTransformation:
    """
    Class to perform transformations on Voice data
    """

    def __init__(self):
        self._logger = Log4j().getLogger()
        self._call_date_col = "call_date"
        self._date_cols_format = "dd-MM-yyyy"
        self._call_date_col_format = "yyyyMMddHHmmss"
        self.time_zone = "Europe/London"

    def generateDerivedColumnsForVoice(self, df):
        """
        Module to generate derived columns from dataframe

        :param df:
        :return:
        """

        try:
            self._logger.info("Generating derived columns for Voice data.")
            transDF = df.withColumn("_temp_datetime_col",
                                    F.to_timestamp(df[self._call_date_col], self._call_date_col_format)) \
                .withColumn("call_date_month", F.date_format(F.col("_temp_datetime_col"), "yyyyMM").cast(IntegerType())) \
                .withColumn("call_date_dt", F.to_date(F.col("_temp_datetime_col"))) \
                .withColumn("call_date_num", F.date_format(F.col("_temp_datetime_col"), "yyyyMMdd").cast(IntegerType())) \
                .withColumn("call_date_hour",
                            F.date_format(F.col("_temp_datetime_col"), "yyyyMMddHH").cast(IntegerType())) \
                .withColumn("_temp_datetime_col_utc",
                            F.to_utc_timestamp(F.col("_temp_datetime_col"), F.lit(self.time_zone))) \
                .withColumn("call_date_time_gmt", F.from_utc_timestamp(F.col("_temp_datetime_col_utc"), "GMT")) \
                .withColumn("call_date_month_gmt",
                            F.date_format(F.col("call_date_time_gmt"), "yyyyMM").cast(IntegerType())) \
                .withColumn("call_date_num_gmt",
                            F.date_format(F.col("call_date_time_gmt"), "yyyyMMdd").cast(IntegerType())) \
                .withColumn("call_date_hour_gmt",
                            F.date_format(F.col("call_date_time_gmt"), "yyyyMMddHH").cast(IntegerType())) \
                .withColumn("call_duration_min", F.lit(F.col("call_duration") / 60).cast(DecimalType(22, 4))) \
                .withColumn("chargeable_used_time_min",
                            F.lit(F.col("chargeable_used_time") / 60).cast(DecimalType(22, 4))) \
                .drop('_temp_datetime_col', '_temp_datetime_col_utc')
            return transDF
        except Exception as ex:
            self._logger.error("Failed to generate derived columns with error: {err}".format(err=ex))

    def convertTargetDataType(self, df: DataFrame, schema: StructType):
        """
        Module to convert Data Type to required format

        :param df: spark dataframe
        :param schema: schema as StructType
        :return:
        """
        # Drop whole file if error occur in converting
        new_df = df
        self._logger.info("Converting data type to required format")

        files_to_ignore = []
        for column in schema:
            if column.name == self._call_date_col:
                new_df = new_df.withColumn(column.name, F.to_timestamp(new_df[column.name], self._call_date_col_format))
            else:
                new_df = new_df.withColumn(column.name, new_df[column.name].cast(column.dataType))
        return new_df


class TopUpDataTransformation:
    """
    Class to perform transformations on TopUp data
    """

    def __init__(self):
        self._logger = Log4j().getLogger()
        self._cdr_time_stamp_col = "cdr_time_stamp"
        self._cdr_date_cols_format = "dd-MM-yyyy"
        self._cdr_time_stamp_col_format = "yyyyMMddHHmmss"
        self.time_zone = "Europe/London"

    def generateDerivedColumnsForTopUp(self, df):
        """
        Module to generate derived columns from dataframe

        :param df:
        :return:
        """

        try:
            self._logger.info("Generating derived columns for TopUp data.")
            transDF = df.withColumn("_temp_datetime_col",
                                    F.to_timestamp(df[self._cdr_time_stamp_col], self._cdr_time_stamp_col_format)) \
                .withColumn("cdr_dt_month", F.date_format(F.col("_temp_datetime_col"), "yyyyMM").cast(IntegerType())) \
                .withColumn("cdr_dt", F.to_date(F.col("_temp_datetime_col"))) \
                .withColumn("cdr_dt_num", F.date_format(F.col("_temp_datetime_col"), "yyyyMMdd").cast(IntegerType())) \
                .withColumn("call_date_hour",
                            F.date_format(F.col("_temp_datetime_col"), "yyyyMMddHH").cast(IntegerType())) \
                .withColumn("_temp_datetime_col_utc",
                            F.to_utc_timestamp(F.col("_temp_datetime_col"), F.lit(self.time_zone))) \
                .withColumn("cdr_dt_gmt", F.from_utc_timestamp(F.col("_temp_datetime_col_utc"), "GMT")) \
                .withColumn("cdr_dt_month_gmt",
                            F.date_format(F.col("cdr_dt_gmt"), "yyyyMM").cast(IntegerType())) \
                .withColumn("cdr_dt_num_gmt",
                            F.date_format(F.col("cdr_dt_gmt"), "yyyyMMdd").cast(IntegerType())) \
                .withColumn("cdr_dt_hour_gmt",
                            F.date_format(F.col("cdr_dt_gmt"), "yyyyMMddHH").cast(IntegerType())) \
                .drop('_temp_datetime_col', '_temp_datetime_col_utc')

            ex_date_DF = transDF.withColumn("expiry_DATE_derived",
                                            F.coalesce(transDF['Voucher_Onnet_Mins_ExpDt'],
                                                       transDF['Voucher_Onnet_Sms_ExpDt'],
                                                       transDF['Voucher_Offnet_Sms_ExpDt1'],
                                                       transDF['Voucher_Offnet_Mins_ExpDt2'],
                                                       transDF['Voucher_Offnet_Sms_ExpDt2'],
                                                       transDF['Voucher_Offnet_Mins_ExpDt3'],
                                                       transDF['Voucher_Offnet_Sms_ExpDt3'],
                                                       transDF['Voucher_Free_DataExp'],
                                                       transDF['Voucher_Onnet_MT_Expiry_date'],
                                                       transDF['Voucher_Offnet_MT_Expiry_date'],
                                                       transDF['Free_minutes_expiry_date'],
                                                       transDF['Free_SMS_expiry_date'],
                                                       transDF['Free_Offnet_Minutes_Expiry_Date'],
                                                       transDF['Free_Offnet_SMS_Expiry_Date'],
                                                       transDF['Free_OffNet2_Minutes_expiry_date'],
                                                       transDF['Free_OffNet2_SMS_expiry_date'],
                                                       transDF['Free_OffNet3_Minutes_expiry_date'],
                                                       transDF['Free_OffNet3_SMS_expiry_date'],
                                                       transDF['Free_Data_ExpiryDate']))

            newExDF = ex_date_DF.withColumn("expiry_DATE_derived",
                                            F.to_date(F.unix_timestamp(F.col('expiry_DATE_derived'), 'dd-MM-yyyy')
                                                      .cast("timestamp")))
            ex_date_num_DF = newExDF.withColumn("expiry_DATE_derived_num",
                                                F.date_format(F.col("expiry_DATE_derived"), "yyyyMMdd")
                                                .cast(IntegerType()))
            return ex_date_num_DF
        except Exception as ex:
            self._logger.error("Failed to generate derived columns with error: {err}".format(err=ex))

    def convertTargetDataType(self, df: DataFrame, schema: StructType):
        """
        Module to convert Data Type to required format

        :param df: spark dataframe
        :param schema: schema as StructType
        :return:
        """
        # Drop whole file if error occur in converting
        new_df = df
        self._logger.info("Converting data type to required format")

        files_to_ignore = []
        for column in schema:
            if column.name == self._cdr_time_stamp_col:
                new_df = new_df.withColumn(column.name,
                                           F.to_timestamp(new_df[column.name], self._cdr_time_stamp_col_format))
            else:
                new_df = new_df.withColumn(column.name, new_df[column.name].cast(column.dataType))
        return new_df


class DataTransformation:

    def __init__(self):
        self._logger = Log4j().getLogger()
        self.default_value_dict = {'string': '0', 'number': 0, 'date': '1970-01-01', 'datetime': '1970-01-01 00:00:00'}

    def readSourceFile(self, spark, path, structtype: StructType, batchid, checkSumColumns=[],
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
                file_name = file
                file = path + file
                print(file)
                src_schema_string = []
                for elem in structtype:
                    src_schema_string.append(StructField(elem.name, StringType()))
                df_source = spark.read.option("header", "false").option("dateFormat", 'dd-MM-yyyy') \
                    .schema(StructType(src_schema_string)).csv(file)
                df_trimmed = self.trimAllCols(df_source).withColumn("unique_id", F.monotonically_increasing_id())
                df_cleaned_checksum = self.cleanDataForChecksum(df_trimmed)
                df_checksum = df_cleaned_checksum. \
                    withColumn("rec_checksum",
                               F.md5(
                                   F.concat_ws(",", *checkSumColumns))).select("unique_id", "rec_checksum")

                df_with_checksum = df_trimmed.join(df_checksum, on=["unique_id"]).drop("unique_id")

                df_trans = df_with_checksum \
                    .withColumn("filename", F.lit(file_name)) \
                    .withColumn("batch_id", F.lit(batchid).cast(IntegerType())) \
                    .withColumn("created_date", F.current_timestamp())
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
                                                 F.count(dataFrame[checksumColumn]).over(windowspec).cast(
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
            df_source = dataFrame.withColumn("duplicate", F.row_number().over(windowspec).cast(IntegerType()))
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

    def getLateOrNormalCdr(self, dataFrame: DataFrame, integerDateColumn, dateRange) -> DataFrame:
        """
        :parameter dataFrame- source as dataFrame
        :parameter integerDateColumn - numeric column name of date column
        :parameter dateRange
        :return dataframe with new derived columns
        """
        try:
            self._logger.info("Identifying late and normal records within source")
            df_normalOrLate = dataFrame.withColumn("normalOrlate",
                                                   F.when(F.col(integerDateColumn) <= int(dateRange),
                                                          "Late").otherwise(
                                                       "Normal"))
            return df_normalOrLate
        except Exception as ex:
            self._logger.error("Failed to return late and normal records : {error}".format(error=ex))

    def checkDuplicate(self, dfSource: DataFrame, dfRedshift: DataFrame) -> DataFrame:
        """

        :parameter dfSource - get from source file
        :parameter dfRedshift - reading data from redshift late CDR or data mart db
        :return dataframe with new column weather record exist in dfRedshift
        """
        try:
            self._logger.info("Identifying db duplicate within source")
            dfDB = dfRedshift.select(dfRedshift["rec_checksum"])
            dfjoin = dfSource.join(dfDB, dfSource["rec_checksum"] == dfDB["rec_checksum"], "left_outer") \
                .withColumn("newOrDupl",
                            F.when(dfSource["rec_checksum"] == dfDB["rec_checksum"], "Duplicate").otherwise(
                                "New"))
            dfnormalOrDuplicate = dfjoin.drop(dfDB["rec_checksum"])
            return dfnormalOrDuplicate
        except Exception as ex:
            self._logger.error("Failed to return unique records : {error}".format(error=ex))

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
