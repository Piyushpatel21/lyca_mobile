

from pyspark.sql import SparkSession, DataFrame

from pyspark.sql.types import IntegerType, StringType, StructType, StructField


def getDataFrmae(sparkSession: SparkSession, filename, cnt) -> DataFrame:
    schema = StructType([StructField('filename', StringType(), True), StructField(cnt, IntegerType(), True)])
    data = [(filename, cnt)]
    print(data)
    rdd = sparkSession.sparkContext.parallelize(data)
    return sparkSession.createDataFrame(rdd, schema)


sparkSession = SparkSession.builder.master("local").appName("appname").getOrCreate()
recourdCount = getDataFrmae(sparkSession, 'sample', '20')
recourdCount.show(20, False)