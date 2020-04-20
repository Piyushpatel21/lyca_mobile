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


class Log:

    def __init__(self, msg):
        self.msg = msg
        logFormatter = '%(asctime)s %(levelname)s [%(module)s] %(funcName)s %(lineno)s - %(message)s'
        logging.basicConfig(format=logFormatter)
        self._logger = None
        self._logger = logging.getLogger(__name__)

    def get_logger(self):
        """:return logger"""
        return self._logger

    def set_level(self, level=None):
        """:parameter log level type
           :return logger with a specific log level"""
        if level == "INFO":
            # self._logger.setLevel(self.log4j.Level.INFO) if self.spark else
            self._logger.setLevel(logging.INFO)
        elif level == "WARN":
            # self._logger.setLevel(self.log4j.Level.WARN) if self.spark else
            self._logger.setLevel(logging.WARN)
        elif level == "DEBUG":
            # self._logger.setLevel(self.log4j.Level.DEBUG) if self.spark else
            self._logger.setLevel(logging.DEBUG)
        elif level == "ERROR":
            # self._logger.setLevel(self.log4j.Level.ERROR) if self.spark else
            self._logger.setLevel(logging.ERROR)
        else:
            # self._logger.setLevel(self.log4j.Level.WARN) if self.spark else
            self._logger.setLevel(logging.WARN)
            self._logger.warning("%s level not found. Only available levels are INFO, WARN, DEBUG, ERROR. "
                                 "Using WARN level as default", level)

    def debug(self, setLevel):
        rootLogger = self.get_logger()
        self.set_level(setLevel)
        consoleHandler = logging.StreamHandler()
        rootLogger.addHandler(consoleHandler)
        logging.warning(self.msg)


Log("Testing my job").debug("INFO")