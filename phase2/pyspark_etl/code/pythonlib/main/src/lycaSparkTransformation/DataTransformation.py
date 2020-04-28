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
from pyspark.sql import DataFrame, Window, Column
from functools import reduce
from pyspark.sql import functions as py_function
from pyspark.sql.types import StructType
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql.types import IntegerType, StringType, DateType


class DataTransformation:

    @staticmethod
    def readSourceFile(spark, path, structtype: StructType, checkSumColumns=[], fList=[]) -> DataFrame:
        """ :parameter spark
            :parameter path of source files
            :parameter structtype - schema for source file
            :parameter checkSumColumns - list of checksum columns
            :parameter fList of source files
            :return union of all source files"""
        try:
            df_list = []
            for file in fList:
                file_identifier = str(file).lower().replace(".cdr", "")
                df_source = "df_" + file_identifier
                print("Reading source file =====> " + file_identifier)
                file = path + file
                df_source = spark.read.option("header", "false").schema(structtype).csv(file)
                df_trans = df_source.withColumn("checksum",
                                                py_function.md5(py_function.concat_ws(",", *checkSumColumns))) \
                    .withColumn("filename", py_function.lit(file_identifier))
                df_list.append(df_trans)
            print("<============ Merge all DataFrame using Union ============>")
            return reduce(DataFrame.union, df_list)
        except Exception:
            print("Error in reading files to create DataFrame" + Exception)

    @staticmethod
    def getCheckSumColumns(JsonPath) -> []:
        """ :parameter JsonPath schema file path
            :return list of column which is part of building checksum column"""
        try:
            data = JsonProcessor.json_parser(JsonPath)
            checkColList = []
            for col in data:
                if col["checkSum"]:
                    checkColList.append(col["column_name"])
            return checkColList
        except ValueError:
            print('Decoding JSON has failed')

    @staticmethod
    def getTgtColumns(JsonPath) -> []:
        """ :parameter JsonPath schema file path
            :return list of column which is part of building checksum column"""
        try:
            data = JsonProcessor.json_parser(JsonPath)
            colList = []
            for col in data:
                colList.append(col["column_name"])
            return colList
        except ValueError:
            print('Decoding JSON has failed')

    @staticmethod
    def getDuplicates(dataFrame: DataFrame, checksumColumn) -> DataFrame:
        """:parameter - DataFrame, checkSum column name
           :return duplicate record with in dataFrame basis on checksum column"""
        windowspec = Window.partitionBy(dataFrame[checksumColumn]).orderBy(dataFrame[checksumColumn].desc())
        df_duplicates = dataFrame.withColumn("duplicate",
                                             py_function.count(dataFrame[checksumColumn]).over(windowspec).cast(
                                                 IntegerType())) \
            .filter('duplicate > 1')
        return df_duplicates

    @staticmethod
    def getUnique(dataFrame: DataFrame, checksumColumn) -> DataFrame:
        """:parameter - DataFrame, checkSum column name
            :return unique record with in dataFrame basis on checksum column"""
        windowspec = Window.partitionBy(dataFrame[checksumColumn]).orderBy(dataFrame[checksumColumn])
        df_source = dataFrame.withColumn("duplicate", py_function.row_number().over(windowspec).cast(IntegerType()))
        df_unique_records = df_source.filter(df_source['duplicate'] == 1).drop(df_source['duplicate'])
        return df_unique_records

    @staticmethod
    def getPrevRangeDate(mnthOrdaily=None, noOfdaysOrMonth=None):
        """:parameter - monthly or daily and no. of month or days
           :return difference date between current date and given no. of days and month"""
        if mnthOrdaily == 'daily':
            d = datetime.today() + relativedelta(days=-noOfdaysOrMonth)
            check_date = d.strftime("%Y%m%d")
            return check_date
        elif mnthOrdaily == 'monthly':
            d = datetime.today() + relativedelta(months=-noOfdaysOrMonth)
            check_date = d.strftime("%Y%m%d")
            return check_date
        else:
            d = datetime.today() + relativedelta(days=-1)
            check_date = d.strftime("%Y%m%d")
            return check_date

    @staticmethod
    def getLateOrNormalCdr(dataFrame: DataFrame, dateColumn, formattedDateColumn, integerDateColumn,
                           dateRange) -> DataFrame:
        """:parameter source as dataFrame
           :parameter date column
           :parameter formatted Date Column name
           :parameter numeric column name of date column
           :return dataframe with new derived columns"""
        df_event_date = dataFrame.withColumn(integerDateColumn,
                                             py_function.substring(py_function.col(dateColumn), 0, 8).cast(
                                                 IntegerType()))
        df_normalOrLate = df_event_date.withColumn(formattedDateColumn, py_function.to_date(
            py_function.to_date(py_function.col(integerDateColumn).cast(StringType()), 'yyyyMMdd'), 'yyyy-MM-dd')) \
            .withColumn("normalOrlate",
                        py_function.when(py_function.col(integerDateColumn) < int(dateRange), "Late").otherwise(
                            "Normal"))
        return df_normalOrLate

    @staticmethod
    def getDbDuplicate(dfSource: DataFrame, dfRedshift: DataFrame) -> DataFrame:
        """:parameter dfSource - get from source file
           :parameter dfRedshift - reading data from redshift late CDR or data mart db
           :return dataframe with new column weather record exist in dfRedshift"""
        dfDB = dfRedshift.select(dfRedshift["checksum"])
        dfjoin = dfSource.join(dfDB, dfSource["checksum"] == dfDB["checksum"], "left_outer") \
            .withColumn("newOrDupl",
                        py_function.when(dfSource["checksum"] == dfDB["checksum"], "Duplicate").otherwise("New"))
        dfnormalOrDuplicate = dfjoin.drop(dfDB["checksum"])
        return dfnormalOrDuplicate

    @staticmethod
    def writeToS3(dataFrame: DataFrame, run_date, path, tgtColmns=[]):
        # dataFrame.withColumn("rn", py_function.monotonically_increasing_id()).show(150, False)
        dataFrame.repartition(1).select(*tgtColmns).write.option("header", "true").format('csv').mode('append').option('sep', ',').save(path)