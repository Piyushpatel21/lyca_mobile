########################################################################
# description     : Application Log4j                                  #
# author          : Naren K(narendra.kumar@cloudwick.com),             #
#                   Tejveer Singh(tejveer.singh@cloudwick.com)         #
#                   Shubhajit Saha(shubhajit.saha@cloudwick.com)       #
# contributor     :                                                    #
# version         : 1.0                                                #
# notes           :                                                    #
########################################################################

import logging
from parent_utils.glue_spark import GlueSpark


class Log4j:
    """
    Wrapper class for log4j object
    """

    def __init__(self, spark=None):
        """
        Initialise Log4j class for spark program
        :param spark: Instance of spark session
        >>> logger = Log4j(spark)
        """

        self._logger = None
        self.spark = spark
        self._is_framework_spark = bool(spark)
        if spark:
            if spark.conf.get('spark.master') == "yarn":
                glue_spark = GlueSpark()
                self._logger = glue_spark.getLogger()
            else:
                conf = self.spark.sparkContext.getConf()
                app_name = conf.get("spark.app.name", "")
                app_id = conf.get("spark.app.id", "")
                self.log4j = self.spark._jvm.org.apache.log4j
                log_name = "[" + app_name + " " + app_id + "]"
                self._logger = self.log4j.LogManager.getLogger(log_name)
        else:
            _format = '%(asctime)s %(levelname)s [%(module)s] %(funcName)s %(lineno)s - %(message)s'
            logging.basicConfig(format=_format)
            self._logger = logging.getLogger(__name__)

    def getLogger(self):
        """
        :return:logger instance
        """
        return self._logger

    # pylint: disable=expression-not-assigned
    def setLevel(self, level=None):
        """
        Set the logging level of the logger
        :param level: Allowed values are: INFO, WARN, DEBUG, ERROR
        :return:
        """
        if level == "INFO":
            self._logger.setLevel(self.log4j.Level.INFO) if self.spark else self._logger.setLevel(logging.INFO)
        elif level == "WARN":
            self._logger.setLevel(self.log4j.Level.WARN) if self.spark else self._logger.setLevel(logging.WARN)
        elif level == "DEBUG":
            self._logger.setLevel(self.log4j.Level.DEBUG) if self.spark else self._logger.setLevel(logging.DEBUG)
        elif level == "ERROR":
            self._logger.setLevel(self.log4j.Level.ERROR) if self.spark else self._logger.setLevel(logging.ERROR)
        else:
            self._logger.setLevel(self.log4j.Level.WARN) if self.spark else self._logger.setLevel(logging.WARN)
            self._logger.warning("%s level not found. Only available levels are INFO, WARN, DEBUG, ERROR. "
                                 "Using WARN level as default", level)

    def error(self, message):
        """
        :param message: Message to log as error
        :return:
        >>> self._lo.error("This is error message")
        """
        self._logger.error(message)

    def info(self, message):
        """
        Logs message with level INFO
        :param message: Message to log as info
        :return:
        >>> logger.error("This is info message")
        """
        self._logger.info(message)

    def warn(self, message):
        """
        Logs message with level WARN
        :param message: Message to log as warn
        :return:

        >>> logger.error("This is warn message")
        """
        self._logger.warning(message)