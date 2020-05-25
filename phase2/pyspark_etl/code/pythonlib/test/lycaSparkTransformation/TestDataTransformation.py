from phase2.pyspark_etl.code.pythonlib.main.src.lycaSparkTransformation.SparkSessionBuilder import SparkSessionBuilder
from phase2.pyspark_etl.code.pythonlib.main.src.lycaSparkTransformation.DataTransformation import DataTransformation
from pyspark.sql.types import *
from pyspark.sql.functions import col, monotonically_increasing_id, lit
import datetime


class TestTransformation:
    sparkSessionBuild = SparkSessionBuilder().sparkSessionBuild()
    spark = sparkSessionBuild.get("sparkSession")
    dataTransformation = DataTransformation()

    def testSparkInit(self):
        assert self.spark.version == "2.4.3"

    def testTrimWhiteSpace(self):
        _schema = [StructField("name", StringType()), StructField("id", IntegerType())]
        source_df = self.spark.createDataFrame(
            [("foo ", 1),
             (None, 2),
             ("   foobar", 3)],
            schema=StructType(_schema)
        )

        actual_df = source_df.withColumn(
            "name",
            self.dataTransformation.trimColumn(col("name"))
        )

        expected_df = self.spark.createDataFrame(
            [("foo", 1),
             (None, 2),
             ("foobar", 3)],
            schema=StructType(_schema)
        )

        assert actual_df.collect() == expected_df.collect()

    def testTrimWhiteSpaceString(self):
        _schema = [StructField("name", StringType()),
                   StructField("id", StringType())]
        source_df = self.spark.createDataFrame(
            [("foo ", "1"),
             (None, "2"),
             ("foobar", "3")],
            schema=StructType(_schema)
        )

        actual_df = source_df.withColumn(
            "name",
            self.dataTransformation.trimColumn(col("name"))
        )

        expected_df = self.spark.createDataFrame(
            [("foo", "1"),
             (None, "2"),
             ("foobar", "3")],
            schema=StructType(_schema)
        )

        assert actual_df.collect() == expected_df.collect()

    def testTrimAllWhiteSpace(self):
        _schema = [StructField("name", StringType()),
                   StructField("id", StringType())]
        source_df = self.spark.createDataFrame(
            [("foo ", "1"),
             (None, "2"),
             ("foobar", None)],
            schema=StructType(_schema)
        )

        actual_df = self.dataTransformation.trimAllCols(source_df)

        expected_df = self.spark.createDataFrame(
            [("foo", "1"),
             (None, "2"),
             ("foobar", None)],
            schema=StructType(_schema)
        )

        assert actual_df.collect() == expected_df.collect()

    def testFillNull(self):
        _schema = [StructField("name", StringType()),
                   StructField("id", IntegerType()),
                   StructField("date_ex", DateType())]

        source_df = self.spark.createDataFrame(
            [("foo", 1, datetime.date(2020, 5, 12)),
             (None, None, None),
             ("foobar", 3, None)],
            schema=StructType(_schema)
        )

        actual_df = self.dataTransformation.fillNull(source_df)

        expected_df = self.spark.createDataFrame(
            [("foo", 1, datetime.date(2020, 5, 12)),
             ("0", 0, datetime.date(1970, 1, 1)),
             ("foobar", 3, datetime.date(1970, 1, 1))],
            schema=StructType(_schema)
        )

        assert actual_df.collect() == expected_df.collect()

    def testFillNullCustomValues(self):
        _schema = [StructField("name", StringType()),
                   StructField("id", IntegerType()),
                   StructField("date_ex", StringType())]
        source_df = self.spark.createDataFrame(
            [("foo", 1, "2020-05-17"),
             (None, None, None),
             ("foobar", 3, "")],
            schema=StructType(_schema)
        )
        value_dict = {'string': '0', 'number': 0, 'date': '0', 'datetime': '0'}

        actual_df = self.dataTransformation.fillNull(source_df, value_dict)

        expected_df = self.spark.createDataFrame(
            [("foo", 1, "2020-05-17"),
             ("0", 0, "0"),
             ("foobar", 3, "")],
            schema=StructType(_schema)
        )

        assert actual_df.collect() == expected_df.collect()

    def testFillBlanks(self):
        _schema = [StructField("name", StringType()),
                   StructField("id", IntegerType()),
                   StructField("date_ex", StringType())]
        source_df = self.spark.createDataFrame(
            [("foo", 1, "2020-05-17"),
             ("foobar", 3, "")],
            schema=StructType(_schema)
        )
        value_dict = {'string': '0', 'number': 0, 'date': '0', 'datetime': '0'}

        actual_df = self.dataTransformation.fillBlanks(source_df, "0")

        expected_df = self.spark.createDataFrame(
            [("foo", 1, "2020-05-17"),
             ("foobar", 3, "0")],
            schema=StructType(_schema)
        )

        assert actual_df.collect() == expected_df.collect()

    def testFullCleanDf(self):
        _schema = [StructField("name", StringType()),
                   StructField("id", IntegerType())]
        source_df = self.spark.createDataFrame(
            [("foo ", 1),
             (" bar", None),
             ("foobar", 3)],
            schema=StructType(_schema)
        )

        trimmed_df = self.dataTransformation.trimAllCols(source_df)

        no_null_df = self.dataTransformation.fillNull(trimmed_df)

        expected_df = self.spark.createDataFrame(
            [("foo", 1),
             ("bar", 0),
             ("foobar", 3)],
            schema=StructType(_schema)
        )

        assert no_null_df.collect() == expected_df.collect()

    def testCleanDataForChecksum(self):
        _schema = [StructField("name", StringType()),
                   StructField("id", StringType()),
                   StructField("new_id", StringType())]
        source_df = self.spark.createDataFrame(
            [("foo ", "1", 1),
             (" bar", None, 2),
             ("", None, None),
             ("  ", "", 4),
             ("foobar  ", "3", 5)],
            schema=StructType(_schema)
        )

        actual_df = self.dataTransformation.cleanDataForChecksum(self.dataTransformation.trimAllCols(source_df))

        expected_df = self.spark.createDataFrame(
            [("foo", "1", "1"),
             ("bar", "0", "2"),
             ("0", "0", "0"),
             ("0", "0", "4"),
             ("foobar", "3", "5")]
        )

        assert actual_df.collect() == expected_df.collect()

    def testJoinTwoDataFrame(self):
        _schema = [StructField("name", StringType()),
                   StructField("id", StringType())]
        df1 = self.spark.createDataFrame(
            [("foo ", "1"),
             ("bar ", None),
             ("foobar  ", "3")],
            schema=StructType(_schema)
        )

        trimmed_df1 = self.dataTransformation.trimAllCols(df1).withColumn("unique_id", monotonically_increasing_id())

        df2 = trimmed_df1.withColumn("new_col", lit(1)).select("unique_id", "new_col")

        actual_df = trimmed_df1.join(df2, on=["unique_id"]).drop("unique_id")

        print(actual_df.show())

        expected_df = self.spark.createDataFrame(
            [("foo", "1", 1),
             ("bar", None, 1),
             ("foobar", "3", 1)]
        )

        assert actual_df.collect() == expected_df.collect()

    def testErrornousData(self):
        _schema = [StructField("name", StringType()),
                   StructField("id", IntegerType()),
                   StructField("_corrupt_record", StringType())]
        df1 = self.spark.read.csv('../resources/test_error_file.csv', header=True, schema=StructType(_schema))

        actual_df = df1

        print(actual_df.show())

        expected_df = self.spark.createDataFrame(
            [("foo", 1),
             ("bar", None),
             (None, None)]
        )

        assert actual_df.select('name', 'id').collect() == expected_df.collect()
