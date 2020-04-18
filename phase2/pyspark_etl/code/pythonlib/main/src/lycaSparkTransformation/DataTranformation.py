from commonUtils.CommandLineProcessor import CommandLineProcessor
from pyspark.sql import DataFrame, Window, Column
from functools import reduce
from pyspark.sql import functions as py_function
from pyspark.sql.types import StructType
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql.types import IntegerType, StringType, DateType


class DataTranformation:

    # @staticmethod

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
                df_trans = df_source.withColumn("checksum",
                                                py_function.md5(py_function.concat_ws(",", *checkSumColumns))) \
                    .withColumn("file_identifier", py_function.lit(file_identifier))
                df_list.append(df_trans)
            print("<============ Merge all DataFrame using Union ============>")
            return reduce(DataFrame.union, df_list)
        except Exception:
            print("Error in reading files to create DataFrame" + Exception)

    @staticmethod
    def getCheckSumColumns(JsonPath) -> []:
        try:
            data = CommandLineProcessor.json_parser(JsonPath)
            checkColList = []
            for col in data:
                if col["checkSum"]:
                    checkColList.append(col["column_name"])
            return checkColList
        except ValueError:
            print('Decoding JSON has failed')

    @staticmethod
    def getDuplicates(dataFrame: DataFrame, checksumColumn) -> DataFrame:
        windowspec = Window.partitionBy(checksumColumn).orderBy(dataFrame["checksum"].desc())
        df_duplicates = dataFrame.withColumn("duplicate", py_function.row_number().over(windowspec).cast(IntegerType())) \
            .filter('duplicate = 2')
        return df_duplicates

    @staticmethod
    def getUnique(dataFrame: DataFrame, checksumColumn) -> DataFrame:
        windowspec = Window.partitionBy(checksumColumn).orderBy(checksumColumn)
        df_source = dataFrame.withColumn("duplicate", py_function.row_number().over(windowspec).cast(IntegerType()))
        df_unique_records = df_source.filter(df_source['duplicate'] == 1).drop(df_source['duplicate'])
        return df_unique_records

    @staticmethod
    def getPrevRangeDate(mnthOrdaily=None, noOfdaysOrMonth=None):
        if mnthOrdaily == 'daily':
            d = datetime.today() + relativedelta(days=-noOfdaysOrMonth)
            check_date = d.strftime("%Y%m%d")
            return check_date
        elif mnthOrdaily == 'monthly':
            d = datetime.today() + relativedelta(months=-noOfdaysOrMonth)
            check_date = d.strftime("%Y%m%d")
            return check_date

    @staticmethod
    def getLateOrNormalCdr(dataFrame: DataFrame, dateColumn, formattedDateColumn, integerDateColumn,
                           dateRange) -> DataFrame:
        df_event_date = dataFrame.withColumn(integerDateColumn,
                                             py_function.substring(py_function.col(dateColumn), 0, 8).cast(
                                                 IntegerType()))
        df_normalOrLate = df_event_date.withColumn("normalOrlate",
                                                   py_function.when(py_function.col(integerDateColumn) < int(dateRange),
                                                                    "Late").otherwise("Normal")) \
            .withColumn(formattedDateColumn, py_function.to_date(
            py_function.to_date(py_function.col(integerDateColumn).cast(StringType()), 'yyyyMMdd'), 'yyyy-MM-dd'))
        return df_normalOrLate

    @staticmethod
    def writeToS3(dataFrame: DataFrame):
        dataFrame.write.format('csv').option('header', True).mode('overwrite').option('sep', '|').save('../../../../pythonlib/test/resources/output')
