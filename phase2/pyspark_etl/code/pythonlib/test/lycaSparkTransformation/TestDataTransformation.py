from phase2.pyspark_etl.code.pythonlib.main.src.lycaSparkTransformation.SparkSessionBuilder import SparkSessionBuilder
from phase2.pyspark_etl.code.pythonlib.main.src.lycaSparkTransformation.DataTransformation import DataTransformation
from pyspark.sql.types import *
from pyspark.sql.functions import col
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
             ("foobar", 3)],
            schema=StructType(_schema)
        )

        actual_df = source_df.withColumn(
            "name",
            self.dataTransformation.trimWhiteSpaces(col("name"))
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
            self.dataTransformation.trimWhiteSpaces(col("name"))
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
            [("foo", 1, datetime.date(2020, 5, 12)),
             (None, None, None),
             ("foobar", 3, None)],
            schema=StructType(_schema)
        )
        value_dict = {'string': '0', 'number': 0, 'date': '0', 'datetime': '0'}

        actual_df = self.dataTransformation.fillNull(source_df, value_dict)

        expected_df = self.spark.createDataFrame(
            [("foo", 1, datetime.date(2020, 5, 12)),
             ("0", 0, "0"),
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

