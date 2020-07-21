########################################################################
# description     : Building sparkSession                              #
# author          : Naren K(narendra.kumar@cloudwick.com),             #
#                   Tejveer Singh(tejveer.singh@cloudwick.com)         #
#                   Shubhajit Saha(shubhajit.saha@cloudwick.com)       #
# contributor     :                                                    #
# version         : 1.0                                                #
# notes           :                                                    #
########################################################################

from pyspark.sql import SparkSession

from parent_utils.log4j import Log4j
from parent_utils.glue_spark import GlueSpark


class SparkSessionBuilder:
    def __init__(self, master=None, appname=None):
        self.master = master
        self.appname = appname

    def spark_session_build(self):
        """:parameter - appname for each module identification
           :return SparkSession"""
        try:
            if self.master:
                glueSpark = GlueSpark()
                sparkSession = glueSpark.getSpark()
                sparkSession.conf.set("spark.sql.crossJoin.enabled", "true")
                logger = glueSpark.getLogger()
                logger.info("Initialized Glue Context")
                return {
                    "sparkSession": sparkSession,
                    "logger": logger
                }
            else:
                sparkSession = SparkSession.builder.master("local").appName(self.appname).getOrCreate()
                sparkLogger = Log4j(sparkSession)
                sparkLogger.setLevel("INFO")
                logger = sparkLogger.getLogger()
                logger.info("Initialized Local Spark Context")
                logger.info("Spark App Name: {0}".format(sparkSession.conf.get("spark.app.name")))
                return {
                    "sparkSession": sparkSession,
                    "logger": logger
                }
        except Exception as ex:
            print("Failed to launch SparkSession with : {error}".format(error=ex))
