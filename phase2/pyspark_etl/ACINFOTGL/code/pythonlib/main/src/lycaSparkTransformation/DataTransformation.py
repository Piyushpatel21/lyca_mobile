########################################################################
# description     : Spark Transformation, Spark writer                 #
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
from pyspark.sql.types import IntegerType, StringType, DoubleType, LongType, FloatType, DateType, TimestampType
from pyspark.sql.types import StructType, StructField

from commonUtils.JsonProcessor import JsonProcessor
from commonUtils.Log4j import Log4j


class RecycleMasterTransformation:

    def __init__(self):
        self._logger = Log4j().getLogger()
        self._valid_date = "valid_date"
        self._date_cols_format = "yyyy-MM-dd"
        self._timestamp_cols = ["last_usage_date", "activation_date"]
        self._timestamp_cols_format = "yyyy-MM-dd HH:mm:ss"

    def generateDerivedColumnsForRecycleMaster(self, df: DataFrame):
        """
        Module to generate derived columns from dataframe
        :param df:
        :return:
        """

        try:
            return df
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
        for column in schema:
            if column.name == self._valid_date:
                new_df = new_df.withColumn(column.name, F.to_timestamp(new_df[column.name], self._date_cols_format))
            elif column.name in self._timestamp_cols:
                new_df = new_df.withColumn(column.name, F.to_timestamp(new_df[column.name], self._timestamp_cols_format))
            else:
                new_df = new_df.withColumn(column.name, new_df[column.name].cast(column.dataType))
        return new_df


class TrnActivationTransformation:

    def __init__(self):
        self._logger = Log4j().getLogger()
        self._sim_expiry_date = "sim_expirydate"
        self._auth_date = "authdate"
        self._date_cols_format = "yyyy-MM-dd"
        self._timestamp_cols = ["submitdate", "crcheckdate", "preactdate", "authdate"]
        self._timestamp_cols_format = "yyyy-MM-dd HH:mm:ss"

    def generateDerivedColumnsForTrnActivation(self, df: DataFrame):
        """
        Module to generate derived columns from dataframe
        :param df:
        :return:
        """
        try:
            transdf = df.withColumn("_temp_datetime_col", F.to_timestamp(df[self._auth_date], self._timestamp_cols_format)) \
                .withColumn("authdate_month", F.date_format(F.col("_temp_datetime_col"), "yyyyMM").cast(IntegerType())) \
                .drop('_temp_datetime_col')
            return transdf
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
        for column in schema:
            if column.name == self._sim_expiry_date:
                new_df = new_df.withColumn(column.name, F.to_timestamp(new_df[column.name], self._date_cols_format))
            elif column.name in self._timestamp_cols:
                new_df = new_df.withColumn(column.name, F.to_timestamp(new_df[column.name], self._timestamp_cols_format))
            else:
                new_df = new_df.withColumn(column.name, new_df[column.name].cast(column.dataType))
        return new_df


class TrnActivationBundleCostTransformation:

    def __init__(self):
        self._logger = Log4j().getLogger()
        self._timestamp_cols = ["createddate"]
        self._timestamp_cols_format = "yyyy-MM-dd HH:mm:ss"

    def generateDerivedColumnsForTrnActivationBundleCost(self, df: DataFrame):
        """
        Module to generate derived columns from dataframe
        :param df:
        :return:
        """

        try:
            return df
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
        for column in schema:
            if column.name in self._timestamp_cols:
                new_df = new_df.withColumn(column.name, F.to_timestamp(new_df[column.name], self._timestamp_cols_format))
            else:
                new_df = new_df.withColumn(column.name, new_df[column.name].cast(column.dataType))
        return new_df


class MnpPortTransformation:
    def __init__(self):
        self._logger = Log4j().getLogger()
        self._completed_date = "completeddate"
        self._completed_date_col_format = "yyyy-MM-dd HH:mm:ss"

    def generateDerivedColumnsForMnpPort(self, df: DataFrame):
        """
        Module to generate derived columns from dataframe
        :param df:
        :return:
        """

        try:
            self._logger.info("Generating derived columns for SMS data.")
            transdf = df.withColumn("_temp_datetime_col", F.to_timestamp(df[self._completed_date], self._completed_date_col_format)) \
                        .withColumn("completeddate", F.to_date(F.col("_temp_datetime_col"))) \
                        .withColumn("completeddate_month", F.date_format(F.col("_temp_datetime_col"), "yyyyMM").cast(IntegerType())) \
                        .drop('_temp_datetime_col')
            return transdf
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
        for elem in schema:
            if elem.name == self._completed_date:
                new_df = new_df.withColumn(elem.name, F.to_timestamp(new_df[elem.name], self._completed_date_col_format))
            else:
                new_df = new_df.withColumn(elem.name, new_df[elem.name].cast(elem.dataType))
        return new_df


class MnpSpMasterTransformation:

    def __init__(self):
        self._logger = Log4j().getLogger()
        self._last_modified = "lastmodifieddate"
        self._last_modified_date_col_format = "yyyy-MM-dd HH:mm:ss"

    def generateDerivedColumnsForMnpSpMaster(self, df: DataFrame):
        """
        Module to generate derived columns from dataframe
        :param df:
        :return:
        """

        try:
            self._logger.info("Generating derived columns for SMS data.")
            return df
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
        for elem in schema:
            if elem.name == self._last_modified:
                new_df = new_df.withColumn(elem.name, F.to_timestamp(new_df[elem.name], self._last_modified_date_col_format))
            else:
                new_df = new_df.withColumn(elem.name, new_df[elem.name].cast(elem.dataType))
        return new_df


class FirstUsageTransformation:

    def __init__(self):
        self._logger = Log4j().getLogger()
        self._activation_date = "activationdate"
        self._last_usage_date = "lastusagedate"
        self._old_activation_date = "old_activationdate"
        self._first_top_up_date = "firsttopupdate"
        self._top_up_date = "topupdate"
        self._timestamp_date_col_format = "yyyy-MM-dd HH:mm:ss"

    def generateDerivedColumnsForFirstUsage(self, df: DataFrame):
        """
        Module to generate derived columns from dataframe
        :param df:
        :return:
        """

        try:
            self._logger.info("Generating derived columns for SMS data.")
            transdf = df.withColumn(self._activation_date, F.to_timestamp(df[self._activation_date], self._timestamp_date_col_format)) \
                        .withColumn(self._last_usage_date, F.to_timestamp(df[self._last_usage_date], self._timestamp_date_col_format)) \
                        .withColumn(self._old_activation_date, F.to_timestamp(df[self._old_activation_date], self._timestamp_date_col_format)) \
                        .withColumn(self._first_top_up_date, F.to_timestamp(df[self._first_top_up_date], self._timestamp_date_col_format)) \
                        .withColumn(self._top_up_date, F.to_timestamp(df[self._top_up_date], self._timestamp_date_col_format))
            return transdf
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
        for elem in schema:
            if elem.name in (self._activation_date, self._last_usage_date, self._old_activation_date,
                             self._first_top_up_date, self._top_up_date):
                new_df = new_df.withColumn(elem.name, F.to_timestamp(new_df[elem.name], self._timestamp_date_col_format))
            else:
                new_df = new_df.withColumn(elem.name, new_df[elem.name].cast(elem.dataType))
        return new_df


class DSMRetailerTransformation:
    def __init__(self):
        self._logger = Log4j().getLogger()
        self._created_date = "createddate"
        self._updated_date = "updateddate"
        self._virtual_to_real_date = "virtualtorealdate"
        self._pass_expire_date = "passexpiredate"
        self._last_login_date = "lastlogindate"
        self._threshold_excluded_on = "thresholdexcludedon"
        self._spl_retupd_date = "spl_retupd_date"
        self._locked_timestamp = "lockeddatetime"
        self._credit_limit_auth_date = "creditlimitauthdate"
        self._stock_modified_date = "stock_modifieddate"
        self._notificationonoffuploaddate = "notificationonoffuploaddate"
        self._requested_date = "requested_date"
        self._pre_approved_date = "preapproved_date"
        self._approved_date = "approved_date"
        self._pin_locked_date = "pinlockeddate"
        self._ret_auto_frmdt = "ret_auto_frmdt"
        self._ret_auto_todt = "ret_auto_todt"
        self._timestamp_date_col_format = "yyyy-MM-dd HH:mm:ss"

    def generateDerivedColumnsForDSMRetailer(self, df: DataFrame):
        """
        Module to generate derived columns from dataframe
        :param df:
        :return:
        """

        try:
            self._logger.info("Generating derived columns for SMS data.")
            df_trans = df.withColumn("password", F.lit(""))
            return df_trans
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
        for elem in schema:
            if elem.name in (self._created_date, self._updated_date, self._virtual_to_real_date, self._pass_expire_date,
                             self._last_login_date, self._threshold_excluded_on, self._spl_retupd_date,
                             self._locked_timestamp, self._credit_limit_auth_date, self._stock_modified_date,
                             self._notificationonoffuploaddate, self._requested_date, self._pre_approved_date,
                             self._approved_date, self._pin_locked_date, self._ret_auto_frmdt, self._ret_auto_todt):
                new_df = new_df.withColumn(elem.name,
                                           F.to_timestamp(new_df[elem.name], self._timestamp_date_col_format))
            else:
                new_df = new_df.withColumn(elem.name, new_df[elem.name].cast(elem.dataType))
        return new_df


class MNPONORangeTransformation:
    def __init__(self):
        self._logger = Log4j().getLogger()
        self._last_modified_date = "lastmodifieddate"
        self._timestamp_date_col_format = "yyyy-MM-dd HH:mm:ss"

    def generateDerivedColumnsForMNPONORange(self, df: DataFrame):
        """
        Module to generate derived columns from dataframe
        :param df:
        :return:
        """

        try:
            self._logger.info("Generating derived columns for SMS data.")
            return df
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
        for elem in schema:
            if elem.name == self._last_modified_date:
                new_df = new_df.withColumn(elem.name, F.to_timestamp(new_df[elem.name], self._timestamp_date_col_format))
            else:
                new_df = new_df.withColumn(elem.name, new_df[elem.name].cast(elem.dataType))
        return new_df


class MNPLycaspMasterTransformation:
    def __init__(self):
        self._logger = Log4j().getLogger()
        self._last_modified_date = "lastmodifieddate"
        self._timestamp_date_col_format = "yyyy-MM-dd HH:mm:ss"

    def generateDerivedColumnsForMNPLycaspMaster(self, df: DataFrame):
        """
        Module to generate derived columns from dataframe
        :param df:
        :return:
        """

        try:
            self._logger.info("Generating derived columns for SMS data.")
            return df
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
        for elem in schema:
            if elem.name == self._last_modified_date:
                new_df = new_df.withColumn(elem.name, F.to_timestamp(new_df[elem.name], self._timestamp_date_col_format))
            else:
                new_df = new_df.withColumn(elem.name, new_df[elem.name].cast(elem.dataType))
        return new_df


class MNPONOMasterTransformation:
    def __init__(self):
        self._logger = Log4j().getLogger()
        self._last_modified_date = "lastmodifieddate"
        self._timestamp_date_col_format = "yyyy-MM-dd HH:mm:ss"

    def generateDerivedColumnsForMNPONOMaster(self, df: DataFrame):
        """
        Module to generate derived columns from dataframe
        :param df:
        :return:
        """

        try:
            self._logger.info("Generating derived columns for SMS data.")
            return df
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
        for elem in schema:
            if elem.name == self._last_modified_date:
                new_df = new_df.withColumn(elem.name, F.to_timestamp(new_df[elem.name], self._timestamp_date_col_format))
            else:
                new_df = new_df.withColumn(elem.name, new_df[elem.name].cast(elem.dataType))
        return new_df


class SkipMstCustomerTransformation:
    def __init__(self):
        self._logger = Log4j().getLogger()
        self._date_of_creation = "dateofcreation"
        self._activated_dt = "activateddt"
        self._updated_dt = "updateddt"
        self._verified_date = "verifieddate"
        self._pin_assigned_date = "pinassigneddate"
        self._sch_topup_created_dt = "schtopupcreateddt"
        self._sch_topup_activated_dt = "schtopupactivateddt"
        self._sch_topup_updated_dt = "schtopupupdateddt"
        self._auto_start_date = "autostartdate"
        self._auto_end_date = "autoenddate"
        self._redemp_process_date = "redempprocessdate"
        self._doc_expiry_date = "doc_expiry_date"
        self._timestamp_date_col_format = "yyyy-MM-dd HH:mm:ss"

    def generateDerivedColumnsForSkipMstCustomer(self, df: DataFrame):
        """
        Module to generate derived columns from dataframe
        :param df:
        :return:
        """

        try:
            self._logger.info("Generating derived columns for SMS data.")
            return df
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
        for elem in schema:
            if elem.name in (self._date_of_creation, self._activated_dt, self._updated_dt,
                             self._verified_date, self._pin_assigned_date, self._sch_topup_created_dt,
                             self._sch_topup_activated_dt, self._sch_topup_updated_dt,
                             self._auto_start_date, self._auto_end_date, self._redemp_process_date,
                             self._doc_expiry_date, self._timestamp_date_col_format):
                new_df = new_df.withColumn(elem.name, F.to_timestamp(new_df[elem.name], self._timestamp_date_col_format))
            else:
                new_df = new_df.withColumn(elem.name, new_df[elem.name].cast(elem.dataType))
        return new_df


class SkipMstFreeSimCustomerTransformation:

    def __init__(self):
        self._logger = Log4j().getLogger()
        self._date_of_creation = "dateofcreation"
        self._updated_date = "updateddate"
        self._date_of_issue = "dateofissue"
        self._valid_date = "validdate"
        self._timestamp_date_col_format = "yyyy-MM-dd HH:mm:ss"

    def generateDerivedColumnsForSkipMstFreeSimCustomer(self, df: DataFrame):
        """
        Module to generate derived columns from dataframe
        :param df:
        :return:
        """

        try:
            self._logger.info("Generating derived columns for SMS data.")
            return df
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
        for elem in schema:
            if elem.name in (self._date_of_creation, self._updated_date, self._date_of_issue, self._valid_date):
                new_df = new_df.withColumn(elem.name, F.to_timestamp(new_df[elem.name], self._timestamp_date_col_format))
            else:
                new_df = new_df.withColumn(elem.name, new_df[elem.name].cast(elem.dataType))
        return new_df


class TblShopTransactionStatusTransformation:

    def __init__(self):
        self._logger = Log4j().getLogger()
        self._transaction_date = "transactiondate"
        self._timestamp_date_col_format = "yyyy-MM-dd HH:mm:ss"

    def generateDerivedColumnsForTblShopTransactionStatus(self, df: DataFrame):
        """
        Module to generate derived columns from dataframe
        :param df:
        :return:
        """

        try:
            self._logger.info("Generating derived columns for SMS data.")
            return df
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
        for elem in schema:
            if elem.name == self._transaction_date:
                new_df = new_df.withColumn(elem.name, F.to_timestamp(new_df[elem.name], self._timestamp_date_col_format))
            else:
                new_df = new_df.withColumn(elem.name, new_df[elem.name].cast(elem.dataType))
        return new_df


class TrnVoucherActivationTransformation:

    def __init__(self):
        self._logger = Log4j().getLogger()
        self._submit_date = "submitdate"
        self._cr_check_date = "crcheckdate"
        self._pre_act_date = "preactdate"
        self._auth_date = "authdate"
        self._block_date = "blockdate"
        self._record_processed_time = "record_processed_time"
        self._timestamp_date_col_format = "yyyy-MM-dd HH:mm:ss"

    def generateDerivedColumnsForTrnVoucherActivation(self, df: DataFrame):
        """
        Module to generate derived columns from dataframe
        :param df:
        :return:
        """

        try:
            self._logger.info("Generating derived columns for SMS data.")
            return df
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
        for elem in schema:
            if elem.name in (self._submit_date, self._cr_check_date, self._pre_act_date,
                             self._auth_date, self._block_date, self._record_processed_time):
                new_df = new_df.withColumn(elem.name, F.to_timestamp(new_df[elem.name], self._timestamp_date_col_format))
            else:
                new_df = new_df.withColumn(elem.name, new_df[elem.name].cast(elem.dataType))
        return new_df


class TrnVouchersTransformation:

    def __init__(self):
        self._logger = Log4j().getLogger()
        self._create_date = "create_date"
        self._expired_date = "expired_date"
        self._used_date = "useddate"
        self._timestamp_date_col_format = "yyyy-MM-dd HH:mm:ss"

    def generateDerivedColumnsForTrnVouchers(self, df: DataFrame):
        """
        Module to generate derived columns from dataframe
        :param df:
        :return:
        """

        try:
            self._logger.info("Generating derived columns for SMS data.")
            transdf = df.withColumn("_temp_datetime_col", F.to_timestamp(df[self._used_date], self._timestamp_date_col_format)) \
                        .withColumn("useddate_month", F.date_format(F.col("_temp_datetime_col"), "yyyyMM").cast(IntegerType())) \
                        .drop('_temp_datetime_col')
            return transdf
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
        for elem in schema:
            if elem.name in (self._create_date, self._expired_date, self._used_date):
                new_df = new_df.withColumn(elem.name, F.to_timestamp(new_df[elem.name], self._timestamp_date_col_format))
            else:
                new_df = new_df.withColumn(elem.name, new_df[elem.name].cast(elem.dataType))
        return new_df


class TrnReCreditTransformation:

    def __init__(self):
        self._logger = Log4j().getLogger()
        self._request_date = "requestdate"
        self._authorised_date = "authoriseddate"
        self._sms_date = "smsdate"
        self._timestamp_date_col_format = "yyyy-MM-dd HH:mm:ss"

    def generateDerivedColumnsForTrnReCredit(self, df: DataFrame):
        """
        Module to generate derived columns from dataframe
        :param df:
        :return:
        """

        try:
            self._logger.info("Generating derived columns for SMS data.")
            return df
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
        for elem in schema:
            if elem.name in (self._request_date, self._authorised_date, self._sms_date):
                new_df = new_df.withColumn(elem.name, F.to_timestamp(new_df[elem.name], self._timestamp_date_col_format))
            else:
                new_df = new_df.withColumn(elem.name, new_df[elem.name].cast(elem.dataType))
        return new_df


class TrnVouchersUsedTransformation:

    def __init__(self):
        self._logger = Log4j().getLogger()
        self._create_date = "create_date"
        self._expired_date = "expired_date"
        self._used_date = "useddate"
        self._timestamp_date_col_format = "yyyy-MM-dd HH:mm:ss"

    def generateDerivedColumnsForTrnVouchersUsed(self, df: DataFrame):
        """
        Module to generate derived columns from dataframe
        :param df:
        :return:
        """

        try:
            self._logger.info("Generating derived columns for SMS data.")
            transdf = df.withColumn("_temp_datetime_col", F.to_timestamp(df[self._used_date], self._timestamp_date_col_format)) \
                        .withColumn("useddate_month", F.date_format(F.col("_temp_datetime_col"), "yyyyMM").cast(IntegerType())) \
                        .drop('_temp_datetime_col')
            return transdf
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
        for elem in schema:
            if elem.name in (self._create_date, self._expired_date, self._used_date):
                new_df = new_df.withColumn(elem.name, F.to_timestamp(new_df[elem.name], self._timestamp_date_col_format))
            else:
                new_df = new_df.withColumn(elem.name, new_df[elem.name].cast(elem.dataType))
        return new_df


class TrnDebitTransformation:

    def __init__(self):
        self._logger = Log4j().getLogger()
        self._request_date = "requestdate"
        self._authorised_date = "authoriseddate"
        self._timestamp_date_col_format = "yyyy-MM-dd HH:mm:ss"

    def generateDerivedColumnsForTrnDebit(self, df: DataFrame):
        """
        Module to generate derived columns from dataframe
        :param df:
        :return:
        """

        try:
            self._logger.info("Generating derived columns for SMS data.")
            return df
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
        for elem in schema:
            if elem.name in (self._request_date, self._authorised_date):
                new_df = new_df.withColumn(elem.name, F.to_timestamp(new_df[elem.name], self._timestamp_date_col_format))
            else:
                new_df = new_df.withColumn(elem.name, new_df[elem.name].cast(elem.dataType))
        return new_df


class WPTransactionLogTransformation:

    def __init__(self):
        self._logger = Log4j().getLogger()
        self._order_date_time = "orderdatetime"
        self._response_date_time = "responsedatetime"
        self._created_date = "createddate"
        self._timestamp_date_col_format = "yyyy-MM-dd HH:mm:ss"

    def generateDerivedColumnsForWPTransaction(self, df: DataFrame):
        """
        Module to generate derived columns from dataframe
        :param df:
        :return:
        """

        try:
            self._logger.info("Generating derived columns for SMS data.")
            return df
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
        for elem in schema:
            if elem.name in (self._order_date_time, self._response_date_time, self._created_date):
                new_df = new_df.withColumn(elem.name, F.to_timestamp(new_df[elem.name], self._timestamp_date_col_format))
            else:
                new_df = new_df.withColumn(elem.name, new_df[elem.name].cast(elem.dataType))
        return new_df


class ReDTransactionLogTransformation:

    def __init__(self):
        self._logger = Log4j().getLogger()
        self._response_date = "responsedate"
        self._red_order_date_time = "redorderdatetime"
        self._created_date = "createddate"
        self._timestamp_date_col_format = "yyyy-MM-dd HH:mm:ss"

    def generateDerivedColumnsForReDTransactionLog(self, df: DataFrame):
        """
        Module to generate derived columns from dataframe
        :param df:
        :return:
        """

        try:
            self._logger.info("Generating derived columns for SMS data.")
            return df
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
        for elem in schema:
            if elem.name in (self._response_date, self._red_order_date_time, self._created_date):
                new_df = new_df.withColumn(elem.name, F.to_timestamp(new_df[elem.name], self._timestamp_date_col_format))
            else:
                new_df = new_df.withColumn(elem.name, new_df[elem.name].cast(elem.dataType))
        return new_df


class IngenicoTransactionLogTransformation:

    def __init__(self):
        self._logger = Log4j().getLogger()
        self._status_code_change_date_time = "statuscodechangedatetime"
        self._created_date = "createddate"
        self._timestamp_date_col_format = "yyyy-MM-dd HH:mm:ss"

    def generateDerivedColumnsForIngenicoTransactionLog(self, df: DataFrame):
        """
        Module to generate derived columns from dataframe
        :param df:
        :return:
        """

        try:
            self._logger.info("Generating derived columns for SMS data.")
            return df
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
        for elem in schema:
            if elem.name in (self._status_code_change_date_time, self._created_date):
                new_df = new_df.withColumn(elem.name, F.to_timestamp(new_df[elem.name], self._timestamp_date_col_format))
            else:
                new_df = new_df.withColumn(elem.name, new_df[elem.name].cast(elem.dataType))
        return new_df


class SRTTrnVouchersLogTransformation:

    def __init__(self):
        self._logger = Log4j().getLogger()
        self._create_date = "create_date"
        self._expired_date = "expired_date"
        self._used_date = "useddate"
        self._recycled_on = "recycledon"
        self._timestamp_date_col_format = "yyyy-MM-dd HH:mm:ss"

    def generateDerivedColumnsForSRTTrnVouchersLog(self, df: DataFrame):
        """
        Module to generate derived columns from dataframe
        :param df:
        :return:
        """

        try:
            self._logger.info("Generating derived columns for SMS data.")
            transdf = df.withColumn("_temp_datetime_col", F.to_timestamp(df[self._used_date], self._timestamp_date_col_format)) \
                    .withColumn("useddate_month", F.date_format(F.col("_temp_datetime_col"), "yyyyMM").cast(IntegerType())) \
                    .drop('_temp_datetime_col')

            return transdf
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
        for elem in schema:
            if elem.name in (self._create_date, self._expired_date, self._used_date, self._recycled_on):
                new_df = new_df.withColumn(elem.name, F.to_timestamp(new_df[elem.name], self._timestamp_date_col_format))
            else:
                new_df = new_df.withColumn(elem.name, new_df[elem.name].cast(elem.dataType))
        return new_df


class SRTTrnvouchersUsedLogTransformation:

    def __init__(self):
        self._logger = Log4j().getLogger()
        self._create_date = "create_date"
        self._expired_date = "expired_date"
        self._used_date = "useddate"
        self._recycled_on = "recycledon"
        self._timestamp_date_col_format = "yyyy-MM-dd HH:mm:ss"

    def generateDerivedColumnsForSRTTrnvouchersUsedLog(self, df: DataFrame):
        """
        Module to generate derived columns from dataframe
        :param df:
        :return:
        """

        try:
            self._logger.info("Generating derived columns for SMS data.")
            transdf = df.withColumn("_temp_datetime_col", F.to_timestamp(df[self._used_date], self._timestamp_date_col_format)) \
                        .withColumn("useddate_month", F.date_format(F.col("_temp_datetime_col"), "yyyyMM").cast(IntegerType())) \
                        .drop('_temp_datetime_col')
            return transdf
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
        for elem in schema:
            if elem.name in (self._create_date, self._expired_date, self._used_date, self._recycled_on):
                new_df = new_df.withColumn(elem.name, F.to_timestamp(new_df[elem.name], self._timestamp_date_col_format))
            else:
                new_df = new_df.withColumn(elem.name, new_df[elem.name].cast(elem.dataType))
        return new_df


class RecycledFirstUsageTransformation:

    def __init__(self):
        self._logger = Log4j().getLogger()
        self._last_usage_date = "last_usage_date"
        self._activation_date = "activationdate"
        self._registration_date = "registrationdate"
        self._deactivation_date = "deactivationdate"
        self._timestamp_date_col_format = "yyyy-MM-dd HH:mm:ss"

    def generateDerivedColumnsForRecycledFirstUsage(self, df: DataFrame):
        """
        Module to generate derived columns from dataframe
        :param df:
        :return:
        """

        try:
            self._logger.info("Generating derived columns for SMS data.")
            return df
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
        for elem in schema:
            if elem.name in (self._last_usage_date, self._activation_date, self._registration_date, self._deactivation_date):
                new_df = new_df.withColumn(elem.name, F.to_timestamp(new_df[elem.name], self._timestamp_date_col_format))
            else:
                new_df = new_df.withColumn(elem.name, new_df[elem.name].cast(elem.dataType))
        return new_df


class MstSwapImsiTransformation:

    def __init__(self):
        self._logger = Log4j().getLogger()
        self._request_date = "requestdate"
        self._authorised_date = "authoriseddate"
        self._topup_date = "topupdate"
        self._dob = "dob"
        self._doe = "doe"
        self._timestamp_date_col_format = "yyyy-MM-dd HH:mm:ss"

    def generateDerivedColumnsForMstSwapImsi(self, df: DataFrame):
        """
        Module to generate derived columns from dataframe
        :param df:
        :return:
        """

        try:
            self._logger.info("Generating derived columns for SMS data.")
            return df
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
        for elem in schema:
            if elem.name in (self._request_date, self._authorised_date, self._topup_date, self._dob, self._doe):
                new_df = new_df.withColumn(elem.name, F.to_timestamp(new_df[elem.name], self._timestamp_date_col_format))
            else:
                new_df = new_df.withColumn(elem.name, new_df[elem.name].cast(elem.dataType))
        return new_df


class MSTMvnoAccountTransformation:

    def __init__(self):
        self._logger = Log4j().getLogger()
        self._act_date = "actdate"
        self._timestamp_date_col_format = "yyyy-MM-dd HH:mm:ss"

    def generateDerivedColumnsForMSTMvnoAccount(self, df: DataFrame):
        """
        Module to generate derived columns from dataframe
        :param df:
        :return:
        """

        try:
            self._logger.info("Generating derived columns for SMS data.")
            return df
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
        for elem in schema:
            if elem.name == self._act_date:
                new_df = new_df.withColumn(elem.name, F.to_timestamp(new_df[elem.name], self._timestamp_date_col_format))
            else:
                new_df = new_df.withColumn(elem.name, new_df[elem.name].cast(elem.dataType))
        return new_df


class MstFreeSimCustActivationTransformation:

    def __init__(self):
        self._logger = Log4j().getLogger()
        self._activated_date = "activateddate"
        self._request_date = "requestdate"
        self._processed_date = "processeddate"
        self._blocked_date = "blockeddate"
        self._nus_expire_date = "nus_expiredate"
        self._pin_expire_date = "pin_expire_date"
        self._timestamp_date_col_format = "yyyy-MM-dd HH:mm:ss"

    def generateDerivedColumnsForMstFreeSimCustActivation(self, df: DataFrame):
        """
        Module to generate derived columns from dataframe
        :param df:
        :return:
        """

        try:
            self._logger.info("Generating derived columns for SMS data.")
            transdf = df.withColumn(self._activated_date, F.to_timestamp(df[self._activated_date], self._timestamp_date_col_format)) \
                        .withColumn(self._request_date, F.to_timestamp(df[self._request_date], self._timestamp_date_col_format)) \
                        .withColumn(self._processed_date, F.to_timestamp(df[self._processed_date], self._timestamp_date_col_format)) \
                        .withColumn(self._blocked_date, F.to_timestamp(df[self._blocked_date], self._timestamp_date_col_format)) \
                        .withColumn(self._nus_expire_date, F.to_timestamp(df[self._nus_expire_date], self._timestamp_date_col_format)) \
                        .withColumn(self._pin_expire_date, F.to_timestamp(df[self._pin_expire_date], self._timestamp_date_col_format))
            return transdf
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
        for elem in schema:
            if elem.name in (self._activated_date, self._request_date, self._processed_date,
                             self._blocked_date, self._nus_expire_date, self._pin_expire_date):
                new_df = new_df.withColumn(elem.name, F.to_timestamp(new_df[elem.name], self._timestamp_date_col_format))
            else:
                new_df = new_df.withColumn(elem.name, new_df[elem.name].cast(elem.dataType))
        return new_df


class MstFreeSimCustomerAddressTransformation:

    def __init__(self):
        self._logger = Log4j().getLogger()

    def generateDerivedColumnsForMstFreeSimCustomerAddress(self, df: DataFrame):
        """
        Module to generate derived columns from dataframe
        :param df:
        :return:
        """

        try:
            self._logger.info("Generating derived columns for SMS data.")
            transdf = df
            return transdf
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
        for elem in schema:
            new_df = new_df.withColumn(elem.name, new_df[elem.name].cast(elem.dataType))
        return new_df


class DataTransformation:

    def __init__(self):
        self._logger = Log4j().getLogger()
        self.default_value_dict = {'string': '0', 'number': 0, 'date': '1970-01-01', 'datetime': '1970-01-01 00:00:00'}

    def readSourceFile(self, spark, path, structtype: StructType, batchid, encoding, checkSumColumns=[],
                       fList=[]) -> DataFrame:
        """ :parameter spark
            :parameter path of source files
            :parameter structtype - schema for source file
            :parameter checkSumColumns - list of checksum columns
            :parameter fList of source files
            :return union of all source files"""
        try:
            self._logger.info("Started reading source files")
            src_schema_string = []
            for elem in structtype:
                src_schema_string.append(StructField(elem.name, StringType()))

            # full_path_list = [path + file for file in fList]
            full_path_list = fList

            self._logger.info("Reading from file list: {list}".format(list=full_path_list))

            df_source_all = spark.read.option("header", "true").option("multiLine", "true").option("encoding", encoding) \
                .schema(StructType(src_schema_string)).csv(full_path_list)

            df_source = df_source_all.withColumn("filename", F.input_file_name()) \
                .withColumn("filename", F.reverse(F.split('filename', '/'))[0])

            df_trimmed = self.trimAllCols(df_source).withColumn("unique_id", F.monotonically_increasing_id())
            df_cleaned_checksum = self.cleanDataForChecksum(df_trimmed)
            df_checksum = df_cleaned_checksum. \
                withColumn("rec_checksum",
                           F.md5(
                               F.concat_ws(",", *checkSumColumns))).select("unique_id", "rec_checksum")

            df_with_checksum = df_trimmed.join(df_checksum, on=["unique_id"]).drop("unique_id")
            df_trans = df_with_checksum \
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

    def markAllRowNormal(self, df: DataFrame):
        return df.withColumn("normalOrlate", F.lit("Normal"))