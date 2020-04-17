from commonUtils.CommandLineProcessor import CommandLineProcessor
from pyspark.sql import DataFrame, Window, WindowSpec
from functools import reduce
from pyspark.sql import functions as py_function
from pyspark.sql.types import StructType
from datetime import datetime
# from dateutil.relativedelta import relativedelta


class DataTranformation:

    @staticmethod
    def readSourceFile(spark, path, structtype: StructType, checkSumColumns=[], fList=[]) -> DataFrame:
        try:
            df_list = []
            for file in fList:
                df_source = "df_" + str(file).lower().replace(".cdr", "")
                file_identifier = df_source
                print("Reading source file =====> " + df_source)
                file = path + file
                df_source = spark.read.option("header", "false").schema(structtype).csv(file)
                df_trans = df_source.withColumn("checksum", py_function.md5(py_function.concat_ws(checkSumColumns))) \
                    .withColumn("file_identifier", py_function.lit(file_identifier))

                df_list.append(df_trans)
            print("<============ Merge all DataFrame using Union ============>")
            return reduce(DataFrame.union, df_list)
        except Exception:
            print("Error in reading files to create DataFrame" + Exception)

    @staticmethod
    def getCheckSumColumns(JsonPath) -> str:
        try:
            data = CommandLineProcessor.json_parser(JsonPath)
            checkSumColumns = ','
            checkColList = []
            for col in data:
                if col["checkSum"]:
                    checkColList.append(col["column_name"])
            return checkSumColumns.join(checkColList)
        except ValueError:
            print('Decoding JSON has failed')

    @staticmethod
    def getDuplicates(dataFrame: DataFrame, dup_col) -> DataFrame:
        windowspec = Window.partitionBy(dup_col)
        df_duplicates = dataFrame.select('*', py_function.count(dup_col).over(windowspec).alias('dupCount')) \
            .where('dupCount > 1').drop('dupCount')
        return df_duplicates

    @staticmethod
    def getUnique(dataFrame: DataFrame, dup_col) -> DataFrame:
        windowSpec = Window.partitionBy(dup_col)
        df_unique_records = dataFrame.select('*', py_function.count(dup_col).over(windowSpec).alias('dupCount')) \
            .where('dupCount = 1').drop('dupCount')
        return df_unique_records

    # @staticmethod
    # def getCheckDate(time_range, no_d_m):
    #     if time_range == 'day':
    #         d = datetime.today() + relativedelta(days=-no_d_m)
    #         check_date = d.strftime("%Y%m%d")
    #         return check_date
    #     elif time_range == 'month':
    #         d = datetime.today() + relativedelta(months=-no_d_m)
    #         check_date = d.strftime("%Y%m%d")
    #         return check_date

    @staticmethod
    def getLateOrNormalCdr(spark, dataFrame: DataFrame, inputColumn, outputColumn, dateRange) -> DataFrame:
        intInput = "int_" + inputColumn
        df_event_date = dataFrame.withColumn(outputColumn, int(py_function.unix_timestamp(inputColumn, 'yyyy-mm-dd')))
        df_normalOrLate = df_event_date.withColumn("normalOrlate",
                                                   py_function.when(intInput < dateRange, "Late").otherwise("Normal"))
        return df_normalOrLate