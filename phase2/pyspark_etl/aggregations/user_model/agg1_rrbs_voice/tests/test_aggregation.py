from code.aggregations import get_aggregation_instance
from code.utils import Agg1SparkSession
from moto import mock_secretsmanager
import boto3
import pytest


class TestAgg1RRBSVoice:

    voice_fact_location = 'assets/fact_rrbs_voice_2020-07-08.csv'
    redshift_credentials = 'Redshift/etl_user'


    @pytest.yield_fixture(scope="module")
    def set_up(self):
        self.mock_sm = mock_secretsmanager()
        self.mock_sm.start()

        print("Creating parameters")
        sm_client = boto3.client(
            'secretsmanager',
            region_name='eu-west-2'
        )
        ssm_response_1 = sm_client.put_secret_value(
                            SecretId=self.redshift_credentials,
                            SecretString='{"username":"bob", "password":"abc123xyz456"}',
                        )

    def test_get_aggregation_instance(self, set_up):
        args = {
            "start_date": "",
            "end_date": "",
            "module": "rrbs",
            "sub_module": "voice",
            "configfile": "../configs/agg1_rrbs_voice_application_properties.json",
            "connfile": "../configs/agg1_connection.json",
            "master": "",
            "code_bucket": ""
        }
        app_name = args.get('module') + '-' + args.get('sub_module')
        spark_session_build = Agg1SparkSession(args.get('master'), app_name).spark_session_build()
        spark = spark_session_build.get("sparkSession")
        logger = spark_session_build.get("logger")
        aggregator = get_aggregation_instance(spark, logger, args)

        # Read data
        df = aggregator.spark.read.csv(self.voice_fact_location, header=True)
        src_cols = aggregator.src_cols
        print(src_cols)
        df = df.select(src_cols)
        all_aggregation = aggregator.perform_distinct_aggregation(df)
        df_distinct_groupby_hour, df_distinct_groupby_daily, df_distinct_groupby_monthly, df_distinct_groupby_yearly = all_aggregation
        df_distinct_groupby_hour.show()
        df_distinct_groupby_monthly.show()
        df_distinct_groupby_yearly.show()
