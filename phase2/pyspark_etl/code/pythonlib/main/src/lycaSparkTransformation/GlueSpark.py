"""
Module to get spark from GlueContext
"""

import sys

try:
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from pyspark.context import SparkContext
except ImportError:
    pass


class GlueSpark:
    """
    Class to get instance of spark session from aws glue
    """

    def __init__(self):
        """
        Initialize the class

        >>> glue_spark = GlueSpark()
        """
        # pylint: disable=invalid-name
        sc = SparkContext()
        self.glue_context = GlueContext(sc)
        self.spark = self.glue_context.spark_session
        self.job = Job(self.glue_context)

        # @params: [JOB_NAME]
        args = getResolvedOptions(sys.argv, ['JOB_NAME'])
        self.job.init(args['JOB_NAME'], args)

    def getSpark(self):
        """
        Returns the spark session instance
        :return: SparkSession

        >>> glue_spark = GlueSpark()
        >>> spark = glue_spark.get_spark()
        """
        return self.spark

    def getGlueContext(self):
        """
        Return glue context instance
        :return: GlueContext

        >>> glue_spark = GlueSpark()
        >>> glue_context = glue_spark.get_glue_context()()
        """
        return self.glue_context

    def getLogger(self):
        """
        Return logger instance for glue
        :return: logger

        >>> glue_spark = GlueSpark()
        """
        return self.glue_context.get_logger()

    def commitJob(self):
        """
        Commit the glue job
        :return:
        """
        self.job.commit()