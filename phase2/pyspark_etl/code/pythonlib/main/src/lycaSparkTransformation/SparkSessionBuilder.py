from pyspark.sql import SparkSession


class SparkSessionBuilder:

    @staticmethod
    def sparkSessionBuild(appName):
        return SparkSession.builder.appName(appName).getOrCreate()
