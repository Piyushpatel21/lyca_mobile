"""
Perform Aggregation Level 2 on RRBS. It performs:
"""

import datetime

from code.sql_queries import agg_count_total_queries, agg_count_usemode_queries
from code.utils import Agg2RedshiftUtils, get_secret, parse_date
from code.utils import Agg2SparkSession, Agg2JsonProcessor, Agg2AwsReader


class Aggregation:
    """
    Perform aggregation on fact table
    """

    def __init__(self, spark, module, agg_type, creation_date_range, start_date, end_date, fmt, conn_file_path, config_file_path, code_bucket, logger):
        """
        :param spark: Spark Context
        :param module: Module of the Aggregation
        :param agg_type: Type of Aggregation
        :param creation_date_range: Range of creation date of which aggregation to be calculated
        :param start_date: Start date in format of yyyyMMdd
        :param end_date: End date in format of yyyyMMdd
        :param fmt: Format of start and end date
        :param conn_file_path: Connection file path
        :param config_file_path: Application configuration file path
        :param code_bucket: Code bucket name
        :param logger: logger
        """

        self.logger = logger
        self.spark = spark
        self.module = module
        self.agg_type = agg_type
        self.creation_date_range = creation_date_range
        self.start = start_date
        self.end = end_date
        self.fmt = fmt
        self.code_bucket = code_bucket
        self.src_cols = ['cli', 'bundle_code', 'call_type', 'cdr_types',
                         'destination_zone_code', 'destination_area_code', 'destination_zone_name',
                         'network_id', 'msg_date_hour', 'msg_date_dt', 'msg_date_num', 'msg_date_month']
        self.hourly_cols = ['cli', 'user_type', 'bundle_code', 'call_type', 'cdr_types',
                            'destination_zone_code', 'destination_area_code', 'destination_zone_name',
                            'network_id', 'msg_date_hour', 'msg_date_dt', 'msg_date_num', 'msg_date_month',
                            'msg_date_year']
        self.daily_cols = ['cli', 'user_type', 'bundle_code', 'call_type', 'cdr_types',
                           'destination_zone_code', 'destination_area_code', 'destination_zone_name',
                           'network_id', 'msg_date_dt', 'msg_date_num', 'msg_date_month', 'msg_date_year']
        self.monthly_cols = ['cli', 'user_type', 'bundle_code', 'call_type', 'cdr_types',
                             'destination_zone_code', 'destination_area_code', 'destination_zone_name',
                             'network_id', 'msg_date_month', 'msg_date_year']
        self.yearly_cols = ['cli', 'user_type', 'bundle_code', 'call_type', 'cdr_types',
                            'destination_zone_code', 'destination_area_code', 'destination_zone_name',
                            'network_id', 'msg_date_year']
        self.conn = self._read_conn_properties(file_path=conn_file_path, code_bucket=self.code_bucket)
        self.logger.info("Received connection properties as: {prop}".format(prop=self.conn))
        self.config = self._read_app_properties(config_file_path, self.module, self.code_bucket)
        self.logger.info("Received application properties as: {prop}".format(prop=self.config))

        dwh_user, dwh_pass = self._get_dwh_user_pass()
        self.rs_utils = Agg2RedshiftUtils(spark=self.spark, dwh_host=self.conn['host'], dwh_port=self.conn['port'],
                                          dwh_db=self.conn['domain'], dwh_user=dwh_user, dwh_pass=dwh_pass,
                                          tmp_dir=self.conn['tmpdir'])

    def _get_dwh_user_pass(self):
        """
        Get the username and password from config file/SecretManager
        """

        if self.conn.get('username') and self.conn.get('password'):
            dwh_user = self.conn.get('username')
            dwh_pass = self.conn.get('password')
        elif self.conn.get('redshiftsecret') and self.conn.get('region'):
            secrets = get_secret(self.conn.get('redshiftsecret'), self.conn.get('region'))
            dwh_user = secrets["username"]
            dwh_pass = secrets["password"]
        else:
            raise Exception("Unable to get the credential for redshift from connecction properties.")

        return dwh_user, dwh_pass

    def _read_json_file(self, file_path, code_bucket=None):
        """
        :param file_path: File path of the json
        :param code_bucket: Code bucket name
        """

        if code_bucket:
            obj = Agg2AwsReader.s3_read_file('s3', code_bucket, file_path)
        else:
            with open(file_path, 'r+') as f:
                obj = f.read()
        return obj

    def _read_app_properties(self, file_path, module, code_bucket=None):
        """
        :param file_path: File path of the json
        :param module: Module name
        :param code_bucket: Code bucket name
        """

        obj = self._read_json_file(file_path, code_bucket=code_bucket)
        return Agg2JsonProcessor.process_app_properties(module, obj)

    def _read_conn_properties(self, file_path, code_bucket=None):
        """
        :param file_path: File path of the conn properties
        :param code_bucket: Code bucket name
        """

        obj = self._read_json_file(file_path, code_bucket=code_bucket)
        return Agg2JsonProcessor.process_conn_properties(obj, 'redshift')

    def read_agg_tables(self, sub_module):
        """
        Reads the aggregated tables
        """

        self.logger.info("Reading aggregated tables for {sub_module}...".format(sub_module=sub_module))

        hourly_df = None
        daily_df = None
        monthly_df = None
        yearly_df = None

        full_hourly_tbl_name = '.'.join(
            [self.config['input'][sub_module]['database'], self.config['input'][sub_module]['hourly_table']])

        full_daily_tbl_name = '.'.join(
            [self.config['input'][sub_module]['database'], self.config['input'][sub_module]['daily_table']])

        full_monthly_tbl_name = '.'.join(
            [self.config['input'][sub_module]['database'], self.config['input'][sub_module]['monthly_table']])

        full_yearly_tbl_name = '.'.join(
            [self.config['input'][sub_module]['database'], self.config['input'][sub_module]['yearly_table']])

        # Get data by creation_date
        if self.creation_date_range:
            self.logger.info("Getting data for creation time from {start} to {end}".format(
                start=self.creation_date_range[0], end=self.creation_date_range[1]
            ))

            self.logger.info("Getting data from hourly table for {sub_module}.".format(sub_module=sub_module))

            # get hourly data from agg table

            query = "SELECT * FROM {table} WHERE created_date_time >= {start} AND created_date_time <= {end}".format(
                table=full_hourly_tbl_name, start=self.start, end=self.end
            )
            hourly_df = self.rs_utils.read_from_redshift_with_query(query)

            self.logger.info("Getting data from daily table for {sub_module}.".format(sub_module=sub_module))

            # get daily data from agg table

            query = "SELECT * FROM {table} WHERE created_date_time >= {start} AND created_date_time <= {end}".format(
                table=full_daily_tbl_name, start=self.start, end=self.end
            )
            daily_df = self.rs_utils.read_from_redshift_with_query(query)

            self.logger.info("Getting data from monthly table for {sub_module}.".format(sub_module=sub_module))

            # get monthly data from agg table

            query = "SELECT * FROM {table} WHERE created_date_time >= {start} AND created_date_time <= {end}".format(
                table=full_monthly_tbl_name, start=self.start, end=self.end
            )
            monthly_df = self.rs_utils.read_from_redshift_with_query(query)

            self.logger.info("Getting data from yearly table for {sub_module}.".format(sub_module=sub_module))

            # get yearly data from agg table

            query = "SELECT * FROM {table} WHERE created_date_time >= {start} AND created_date_time <= {end}".format(
                table=full_yearly_tbl_name, start=self.start, end=self.end
            )
            yearly_df = self.rs_utils.read_from_redshift_with_query(query)

        # Get the data from aggregated table
        if self.fmt.startswith('%Y%m%d'):
            self.logger.info("Getting data from hourly table for {sub_module}.".format(sub_module=sub_module))

            # get hourly data from agg table
            query = "SELECT * FROM {table} WHERE {date_col} >= {start} AND {date_col} <= {end}".format(
                table=full_hourly_tbl_name, start=self.start, end=self.end,
                date_col=self.config['input'][sub_module]['date_col']
            )
            hourly_df = self.rs_utils.read_from_redshift_with_query(query)

            self.logger.info("Getting data from daily table for {sub_module}.".format(sub_module=sub_module))

            # get daily data from agg table
            query = "SELECT * FROM {table} WHERE {date_col} >= {start} AND {date_col} <= {end}".format(
                table=full_daily_tbl_name, start=self.start, end=self.end,
                date_col=self.config['input'][sub_module]['date_col']
            )
            daily_df = self.rs_utils.read_from_redshift_with_query(query)

        if self.fmt.startswith('%Y%m'):

            self.logger.info("Getting data from monthly table for {sub_module}.".format(sub_module=sub_module))

            # get monthly data from agg table
            query = "SELECT * FROM {table} WHERE {month_col} >= {start} AND {month_col} <= {end}".format(
                table=full_monthly_tbl_name, start=self.start[:6], end=self.end[:6],
                month_col=self.config['input'][sub_module]['month_col']
            )
            monthly_df = self.rs_utils.read_from_redshift_with_query(query)

        if self.fmt.startswith('%Y'):

            self.logger.info("Getting data from yearly table for {sub_module}.".format(sub_module=sub_module))

            # get yearly data from agg table
            query = "SELECT * FROM {table} WHERE {year_col} >= {start} AND {year_col} <= {end}".format(
                table=full_yearly_tbl_name, start=self.start[:4],
                end=self.end[:4],
                year_col=self.config['input'][sub_module]['year_col']
            )
            yearly_df = self.rs_utils.read_from_redshift_with_query(query)

        return {'hourly': hourly_df, 'daily': daily_df, 'monthly': monthly_df, 'yearly': yearly_df}

    def perform_aggregation(self, agg_type, sms_tables, voice_tables, gprs_conn_tables, gprs_term_tables):
        """
        Perform the aggregation based on aggregation type
        """

        if agg_type == "count_total":
            sql_queries = agg_count_total_queries
        elif agg_type == "count_usemode":
            sql_queries = agg_count_usemode_queries
        else:
            self.logger.error("Aggregation type {agg_type} is not valid.".format(agg_type=agg_type))
            raise Exception("Aggregation type {agg_type} is not valid.".format(agg_type=agg_type))

        df_agg_hourly = None
        df_agg_daily = None
        df_agg_monthly = None
        df_agg_yearly = None

        if self.fmt.startswith('%Y%m%d') or self.creation_date_range:
            df_agg_hourly = self.spark.sql(sql_queries.hourly_query.format(sms_table=sms_tables['hourly'],
                                                                           voice_table=voice_tables['hourly'],
                                                                           gprs_conn_table=gprs_conn_tables['hourly'],
                                                                           gprs_term_table=gprs_term_tables['hourly']))

            df_agg_daily = self.spark.sql(sql_queries.daily_query.format(sms_table=sms_tables['daily'],
                                                                         voice_table=voice_tables['daily'],
                                                                         gprs_conn_table=gprs_conn_tables['daily'],
                                                                         gprs_term_table=gprs_term_tables['daily']))

        if self.fmt.startswith('%Y%m') or self.creation_date_range:
            df_agg_monthly = self.spark.sql(sql_queries.monthly_query.format(sms_table=sms_tables['monthly'],
                                                                             voice_table=voice_tables['monthly'],
                                                                             gprs_conn_table=gprs_conn_tables['monthly'],
                                                                             gprs_term_table=gprs_term_tables['monthly']))

        if self.fmt.startswith('%Y') or self.creation_date_range:
            df_agg_yearly = self.spark.sql(sql_queries.yearly_query.format(sms_table=sms_tables['yearly'],
                                                                           voice_table=voice_tables['yearly'],
                                                                           gprs_conn_table=gprs_conn_tables['yearly'],
                                                                           gprs_term_table=gprs_term_tables['yearly']))

        return df_agg_hourly, \
               df_agg_daily,\
               df_agg_monthly,\
               df_agg_yearly

    def write_aggregation_data(self, agg_type, frequency, all_aggregation):
        self.logger.info("In write_aggregation_data, writing aggregation for frequency: {freq}".format(freq=frequency))
        for freq, aggregated_df in zip(frequency, all_aggregation):
            if aggregated_df and aggregated_df.count() > 0:
                if freq == 'hourly':
                    self.logger.info("Writing hourly aggregation data.")
                    temp_table = '.'.join([self.config['output'][agg_type]['database'],
                                           self.config['output'][agg_type]['hourly_temp_table']])
                    final_table = '.'.join([self.config['output'][agg_type]['database'],
                                            self.config['output'][agg_type]['hourly_table']])
                    suffix_post_query = """
                    UPDATE {table} SET is_recent = 0 
                    WHERE {hourly_col} >= {start} AND {hourly_col} <= {end} AND
                    created_date_time not in (SELECT MAX(created_date_time) from {table} 
                                               WHERE {hourly_col} >= {start} AND {hourly_col} <= {end})
                    """.format(table=final_table,
                               hourly_col=self.config['output'][agg_type]['hour_col'],
                               start=self.start,
                               end=self.end)

                elif freq == 'daily':
                    self.logger.info("Writing daily aggregation data.")
                    temp_table = '.'.join([self.config['output'][agg_type]['database'],
                                           self.config['output'][agg_type]['daily_temp_table']])
                    final_table = '.'.join([self.config['output'][agg_type]['database'],
                                            self.config['output'][agg_type]['daily_table']])
                    suffix_post_query = """
                    UPDATE {table} SET is_recent = 0 
                    WHERE {daily_col} >= {start} AND {daily_col} <= {end} AND
                    created_date_time not in (SELECT MAX(created_date_time) from {table} 
                                               WHERE {daily_col} >= {start} AND {daily_col} <= {end})
                    """.format(table=final_table,
                               daily_col=self.config['output'][agg_type]['date_col'],
                               start=self.start,
                               end=self.end)

                elif freq == 'monthly':
                    self.logger.info("Writing monthly aggregation data.")
                    temp_table = '.'.join([self.config['output'][agg_type]['database'],
                                           self.config['output'][agg_type]['monthly_temp_table']])
                    final_table = '.'.join([self.config['output'][agg_type]['database'],
                                            self.config['output'][agg_type]['monthly_table']])
                    suffix_post_query = """
                    UPDATE {table} SET is_recent = 0 
                    WHERE {monthly_col} >= {start} AND {monthly_col} <= {end} AND
                    created_date_time not in (SELECT MAX(created_date_time) from {table} 
                                               WHERE {monthly_col} >= {start} AND {monthly_col} <= {end})
                    """.format(table=final_table,
                               monthly_col=self.config['output'][agg_type]['month_col'],
                               start=self.start[0:6],
                               end=self.end[0:6])

                elif freq == 'yearly':
                    self.logger.info("Writing yearly aggregation data.")
                    temp_table = '.'.join([self.config['output'][agg_type]['database'],
                                           self.config['output'][agg_type]['yearly_temp_table']])
                    final_table = '.'.join([self.config['output'][agg_type]['database'],
                                            self.config['output'][agg_type]['yearly_table']])
                    suffix_post_query = """
                    UPDATE {table} SET is_recent = 0 
                    WHERE {yearly_col} >= {start} AND {yearly_col} <= {end} AND
                    created_date_time not in (SELECT MAX(created_date_time) from {table} 
                                               WHERE {yearly_col} >= {start} AND {yearly_col} <= {end})
                    """.format(table=final_table,
                               yearly_col=self.config['output'][agg_type]['year_col'],
                               start=self.start[0:4],
                               end=self.end[0:4])

                else:
                    self.logger.error("Frequency of type {freq} is not valid.".format(freq=freq))
                    raise Exception("Frequency of type {freq} is not valid.".format(freq=freq))

                # Write to redshift
                self.rs_utils.write_to_redshift_with_temp_table(
                    temp_table=temp_table,
                    final_table=final_table,
                    cols=aggregated_df.columns,
                    df=aggregated_df,
                    suffix_post_query=suffix_post_query
                )
            else:
                self.logger.info("No data of frequency type {freq} available.".format(freq=freq))

        self.logger.info("In write_aggregation_data, finished writing data to redshift.")


def get_aggregation_instance(spark, logger, args):
    """
    Creates instance of aggregation class
    """
    # Check if input is correct
    if args.get('start_date') and args.get('end_date'):
        try:
            fmt1, value1 = parse_date(args.get('start_date'))
            fmt2, value2 = parse_date(args.get('end_date'))
            if fmt1 != fmt2:
                raise ValueError("start_date and end_date must be of same format.")
            elif args.get('start_date') > args.get('end_date'):
                raise ValueError("start_date should be less than or equal to end_date.")

        except ValueError as ve:
            raise Exception(ve)
        else:
            start_date = args.get('start_date')
            end_date = args.get('end_date')
            creation_date_range = None
            fmt = fmt1
    else:
        datetime.datetime.combine(datetime.date.today(), datetime.datetime.min.time())
        start_datetime = datetime.datetime.combine(datetime.date.today() - datetime.timedelta(days=1)
                                                   , datetime.datetime.min.time())
        end_datetime = datetime.datetime.combine(datetime.date.today() - datetime.timedelta(days=1)
                                                 , datetime.datetime.max.time())
        start_date = start_datetime.strftime('%Y%m%d')
        end_date = end_datetime.strftime('%Y%m%d')
        creation_date_range = (start_datetime, end_datetime)
        fmt = '%Y%m%d'

    config_file_path = args.get('configfile')
    conn_file_path = args.get('connfile')
    code_bucket = args.get('code_bucket')
    module = args.get('module')
    agg_type = args.get('agg_type')

    logger.info("Starting aggregation job for {module} of type {type}.".format(module=module,
                                                                               type=agg_type))

    aggregator = Aggregation(spark, module, agg_type, creation_date_range, start_date, end_date, fmt, conn_file_path,
                             config_file_path, code_bucket, logger)

    return aggregator


def start_execution(args):
    """
    :param args: All the arguments
    """

    app_name = args.get('module') + '-' + args.get('agg_type')
    spark_session_build = Agg2SparkSession(args.get('master'), app_name).spark_session_build()
    spark = spark_session_build.get("sparkSession")
    logger = spark_session_build.get("logger")

    aggregator = get_aggregation_instance(spark, logger, args)

    aggregator.logger.info("Starting execution.")

    # Read aggregated tables
    sms = aggregator.read_agg_tables('sms')
    voice = aggregator.read_agg_tables('voice')
    gprs_conn = aggregator.read_agg_tables('gprs_conn')
    gprs_term = aggregator.read_agg_tables('gprs_term')

    # Register table
    # SMS
    sms_tables = {}
    for table_type, df in sms.items():
        if df:
            df.createOrReplaceTempView('sms_' + table_type)
            spark.table('sms_' + table_type).persist()
            sms_tables[table_type] = "sms_" + table_type
        else:
            sms_tables[table_type] = None

    # Voice
    voice_tables = {}
    for table_type, df in voice.items():
        if df:
            df.createOrReplaceTempView('voice_' + table_type)
            spark.table('voice_' + table_type).persist()
            voice_tables[table_type] = "voice_" + table_type
        else:
            voice_tables[table_type] = None

    # Gprs connection
    gprs_conn_tables = {}
    for table_type, df in gprs_conn.items():
        if df:
            df.createOrReplaceTempView('gprs_conn_' + table_type)
            spark.table('gprs_conn_' + table_type).persist()
            gprs_conn_tables[table_type] = "gprs_conn_" + table_type
        else:
            gprs_conn_tables[table_type] = None

    # Gprs Termination
    gprs_term_tables = {}
    for table_type, df in gprs_term.items():
        if df:
            df.createOrReplaceTempView('gprs_term_' + table_type)
            spark.table('gprs_term_' + table_type).persist()
            gprs_term_tables[table_type] = "gprs_term_" + table_type
        else:
            gprs_term_tables[table_type] = None

    if args['agg_type'] == 'all':
        agg_type_list = ["count_total", "count_usemode", "count_calltype_user", "count_call_user_usage"]
    else:
        agg_type_list = [args['agg_type']]

    for agg_type in agg_type_list:

        # Perform aggregation
        all_aggregation = aggregator.perform_aggregation(agg_type,
                                                         sms_tables=sms_tables,
                                                         voice_tables=voice_tables,
                                                         gprs_conn_tables=gprs_conn_tables,
                                                         gprs_term_tables=gprs_term_tables)
        frequency = ['hourly', 'daily', 'monthly', 'yearly']

        # Write to Redshift
        aggregator.write_aggregation_data(agg_type, frequency, all_aggregation)

    aggregator.logger.info("Finishing execution.")
