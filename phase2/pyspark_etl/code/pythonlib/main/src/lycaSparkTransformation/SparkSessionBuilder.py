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


class SparkSessionBuilder:

    @staticmethod
    def sparkSessionBuild(appName):
        """:parameter - appname for each module identification
           :return SparkSession"""
        return SparkSession.builder.appName(appName).getOrCreate()
