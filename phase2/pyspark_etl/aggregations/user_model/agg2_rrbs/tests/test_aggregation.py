from code.aggregations import get_aggregation_instance
from code.utils import Agg2SparkSession
from moto import mock_secretsmanager
import boto3
import pytest


class TestAgg1RRBSVoice:

    folder_path = 'assets/'
    data_file_names = {
        'voice': {
            'yearly': folder_path + 'factagg_user_rrbs_voice_yearly_202007270925.csv',
            'monthly': folder_path + 'factagg_user_rrbs_voice_monthly_202007270925.csv',
            'daily': folder_path + 'factagg_user_rrbs_voice_daily_202007270925.csv',
            'hourly': folder_path + 'factagg_user_rrbs_voice_hourly_202007270925.csv'
        },
        'sms': {
            'yearly': folder_path + 'factagg_user_rrbs_sms_yearly_202007270925.csv',
            'monthly': folder_path + 'factagg_user_rrbs_sms_monthly_202007270925.csv',
            'daily': folder_path + 'factagg_user_rrbs_sms_daily_202007270925.csv',
            'hourly': folder_path + 'factagg_user_rrbs_sms_hourly_202007270925.csv'
        },
        'gprs_conn': {
            'yearly': folder_path + 'factagg_user_rrbs_gprs_conn_yearly_202007270925.csv',
            'monthly': folder_path + 'factagg_user_rrbs_gprs_conn_monthly_202007270925.csv',
            'daily': folder_path + 'factagg_user_rrbs_gprs_conn_daily_202007270925.csv',
            'hourly': folder_path + 'factagg_user_rrbs_gprs_conn_hourly_202007270925.csv'
        },
        'gprs_term': {
            'yearly': folder_path + 'factagg_user_rrbs_gprs_term_yearly_202007270925.csv',
            'monthly': folder_path + 'factagg_user_rrbs_gprs_term_monthly_202007270925.csv',
            'daily': folder_path + 'factagg_user_rrbs_gprs_term_daily_202007270925.csv',
            'hourly': folder_path + 'factagg_user_rrbs_gprs_term_hourly_202007270925.csv'
        },
        'dim_tables': {
            'dim_voice_call_category': folder_path + 'ref_rrbs_voice_call_category_202007261312.csv',
            'dim_sms_call_category': folder_path + 'ref_rrbs_sms_call_category_202007261313.csv',
            'dim_data_roamflag': folder_path + 'dim_rrbs_data_roamflag_202007261313.csv',
            'dim_destination_master': folder_path + 'dim_destination_master_202007271057.csv'
        }
    }

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

    def read_data(self, sub_module, aggregator):

        hourly_df = aggregator.spark.read.csv(self.data_file_names[sub_module]['hourly'], header=True)
        daily_df = aggregator.spark.read.csv(self.data_file_names[sub_module]['daily'], header=True)
        monthly_df = aggregator.spark.read.csv(self.data_file_names[sub_module]['monthly'], header=True)
        yearly_df = aggregator.spark.read.csv(self.data_file_names[sub_module]['yearly'], header=True)

        return {'hourly': hourly_df, 'daily': daily_df, 'monthly': monthly_df, 'yearly': yearly_df}

    def read_dim_tables(self, aggregator):

        dim_voice_call_category_df = aggregator.spark.read.csv(self.data_file_names['dim_tables']['dim_voice_call_category'], header=True)
        dim_sms_call_category_df = aggregator.spark.read.csv(self.data_file_names['dim_tables']['dim_sms_call_category'], header=True)
        dim_data_roamflag_df = aggregator.spark.read.csv(self.data_file_names['dim_tables']['dim_data_roamflag'], header=True)
        dim_destination_master_df = aggregator.spark.read.csv(self.data_file_names['dim_tables']['dim_destination_master'], header=True)

        return {'dim_voice_call_category': dim_voice_call_category_df,
                'dim_sms_call_category': dim_sms_call_category_df,
                'dim_data_roamflag': dim_data_roamflag_df,
                'dim_destination_master': dim_destination_master_df}

    def test_get_aggregation_instance(self, set_up):
        args = {
            "start_date": "",
            "end_date": "",
            "module": "rrbs",
            "agg_type": "count_calltype_user",
            "configfile": "../configs/agg2_rrbs_application_properties.json",
            "connfile": "../configs/agg_connection.json",
            "master": "",
            "code_bucket": ""
        }
        app_name = args.get('module') + '-' + args.get('agg_type')
        spark_session_build = Agg2SparkSession(args.get('master'), app_name).spark_session_build()
        spark = spark_session_build.get("sparkSession")
        logger = spark_session_build.get("logger")
        aggregator = get_aggregation_instance(spark, logger, args)
        print(aggregator.creation_date_range)

        # Read data
        sms = self.read_data('sms', aggregator)
        voice = self.read_data('voice', aggregator)
        gprs_conn = self.read_data('gprs_conn', aggregator)
        gprs_term = self.read_data('gprs_term', aggregator)

        # Read dim_tables

        dims = self.read_dim_tables(aggregator)

        # Register table
        # SMS
        sms_tables = {}
        for table_type, df in sms.items():
            if df:
                df.createOrReplaceTempView("sms_" + table_type)
                sms_tables[table_type] = "sms_" + table_type
            else:
                sms_tables[table_type] = None

        # Voice
        voice_tables = {}
        for table_type, df in voice.items():
            if df:
                df.createOrReplaceTempView("voice_" + table_type)
                voice_tables[table_type] = "voice_" + table_type
            else:
                voice_tables[table_type] = None

        # Gprs connection
        gprs_conn_tables = {}
        for table_type, df in gprs_conn.items():
            if df:
                df.createOrReplaceTempView("gprs_conn_" + table_type)
                gprs_conn_tables[table_type] = "gprs_conn_" + table_type
            else:
                gprs_conn_tables[table_type] = None

        # Gprs connection
        gprs_term_tables = {}
        for table_type, df in gprs_term.items():
            if df:
                df.createOrReplaceTempView("gprs_term_" + table_type)
                gprs_term_tables[table_type] = "gprs_term_" + table_type
            else:
                gprs_term_tables[table_type] = None

        # Dimension tables
        dim_tables = {}
        for dim_name, df in dims.items():
            if df:
                df.createOrReplaceTempView(dim_name)
                spark.table(dim_name).persist()
                dim_tables[dim_name] = dim_name
            else:
                dim_tables[dim_name] = None

        if args['agg_type'] == 'all':
            agg_type_list = ["count_call_user_usage", "count_total", "count_usemode",
                             "count_call_dest_usage", "count_calltype_user"]
        else:
            agg_type_list = [args['agg_type']]

        for agg_type in agg_type_list:
            # Perform aggregation
            all_aggregation = aggregator.perform_aggregation(agg_type,
                                                             sms_tables=sms_tables,
                                                             voice_tables=voice_tables,
                                                             gprs_conn_tables=gprs_conn_tables,
                                                             gprs_term_tables=gprs_term_tables,
                                                             dim_tables=dim_tables)
            frequency = ['hourly', 'daily', 'monthly', 'yearly']
            df_hour, df_daily, df_monthly, df_yearly = all_aggregation
            df_hour.show()
            df_daily.show()
            df_monthly.show()
            df_yearly.show()

            print("Count for tables are : ")
            print("Hourly: ", df_hour.count())
            print("Daily: ", df_daily.count())
            print("Monthly: ", df_monthly.count())
            print("Yearly: ", df_yearly.count())
