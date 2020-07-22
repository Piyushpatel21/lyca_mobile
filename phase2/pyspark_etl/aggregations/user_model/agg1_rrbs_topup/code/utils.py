"""
Utils for agg1_rrbs_sms, it contains
* Redshift utility
* Log4j utility
"""

from parent_utils.redshift_utils import RedshiftUtils
from parent_utils.log4j import Log4j
from parent_utils.json_processor import JsonProcessor
from parent_utils.aws_reader import AwsReader
from parent_utils.schema_reader import SchemaReader
from parent_utils.spark_session_builder import SparkSessionBuilder
from parent_utils.secret_manager import get_secret
import datetime


class Agg1RedshiftUtils(RedshiftUtils):
    """
    Inherit class of Redshiftutils
    """

    def __init__(self, spark, dwh_host, dwh_port, dwh_db, dwh_user, dwh_pass, tmp_dir):
        """
        Initialize RedshiftUtils class
        """

        RedshiftUtils.__init__(self, spark, dwh_host, dwh_port, dwh_db, dwh_user, dwh_pass, tmp_dir)


class Agg1Log4j(Log4j):
    """
    Inherits Log4j class
    """

    def __init__(self, spark):
        """
        Initialize Log4j class
        """

        Log4j.__init__(self, spark)


class Agg1JsonProcessor(JsonProcessor):
    """
    Inherits JsonProcessor class
    """

    def __init__(self):
        """
        Initialize JsonProcessor class
        """
        JsonProcessor.__init__(self)


class Agg1AwsReader(AwsReader):
    """
    Inherits AwsReader class
    """

    def __init__(self):
        """
        Initialize AwsReader class
        """

        AwsReader.__init__(self)


class Agg1SchemaReader(SchemaReader):
    """
    Inherits SchemaReader class
    """

    def __init__(self):
        """
        Initialize SchemaReader class
        """

        SchemaReader.__init__(self)


class Agg1SparkSession(SparkSessionBuilder):
    """
    Inherits GlueSpark class
    """

    def __init__(self, master=None, appname=None):
        """
        Initialize GlueSpark class
        """

        SparkSessionBuilder.__init__(self, master=master, appname=appname)


def parse_date(text):
    for fmt in ('%Y%m%d', '%Y%m', '%Y'):
        try:
            value = datetime.datetime.strptime(text, fmt)
            return fmt, value
        except ValueError:
            pass
    raise ValueError('no valid date format found')