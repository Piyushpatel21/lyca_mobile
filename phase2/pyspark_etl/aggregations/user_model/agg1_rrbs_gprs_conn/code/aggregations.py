"""
Perform Aggregation on RRBS GPRS Connection Data. It performs:
* Read from Redshift
* Calculate Metrics
* Save to Redshift
"""

import datetime

from pyspark.sql import DataFrame

from code.sql_queries import *
from code.utils import Agg1RedshiftUtils, get_secret, parse_date
from code.utils import Agg1SparkSession, Agg1JsonProcessor, Agg1AwsReader


class Aggregation:
    """
    Perform aggregation on fact table
    """

    def __init__(self, spark, module, sub_module, creation_date_range, start_date, end_date, fmt, conn_file_path, config_file_path, code_bucket, logger):
        """
        :param spark: Spark Context
        :param module: Module of the Aggregation
        :param sub_module: Sub Module of the Aggregation
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
        self.sub_module = sub_module
        self.creation_date_range = creation_date_range
        self.start = start_date
        self.end = end_date
        self.fmt = fmt
        self.code_bucket = code_bucket
        self.src_cols = ['msisdn', 'bundle_code', 'tariffplan_id', 'cdr_type', 'network_id', 'data_connection_hour',
                         'data_connection_dt', 'data_connection_dt_num', 'data_connection_month']
        self.hourly_cols = ['msisdn', 'user_type', 'bundle_code', 'tariffplan_id', 'cdr_type', 'network_id',
                            'data_connection_hour',
                            'data_connection_dt', 'data_connection_dt_num', 'data_connection_month',
                            'data_connection_year']
        self.daily_cols = ['msisdn', 'user_type', 'bundle_code', 'tariffplan_id', 'cdr_type', 'network_id',
                           'data_connection_dt', 'data_connection_dt_num', 'data_connection_month',
                           'data_connection_year']
        self.monthly_cols = ['msisdn', 'user_type', 'bundle_code', 'tariffplan_id', 'cdr_type', 'network_id',
                             'data_connection_month', 'data_connection_year']
        self.yearly_cols = ['msisdn', 'user_type', 'bundle_code', 'tariffplan_id', 'cdr_type', 'network_id',
                            'data_connection_year']
        self.spark_table_name = "temp_fact_table"
        self.conn = self._read_conn_properties(file_path=conn_file_path, code_bucket=self.code_bucket)
        self.logger.info("Received connection properties as: {prop}".format(prop=self.conn))
        self.config = self._read_app_properties(config_file_path, self.module, self.sub_module, self.code_bucket)
        self.logger.info("Received application properties as: {prop}".format(prop=self.config))

        dwh_user, dwh_pass = self._get_dwh_user_pass()
        self.rs_utils = Agg1RedshiftUtils(spark=self.spark, dwh_host=self.conn['host'], dwh_port=self.conn['port'],
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
            obj = Agg1AwsReader.s3_read_file('s3', code_bucket, file_path)
        else:
            with open(file_path, 'r+') as f:
                obj = f.read()
        return obj

    def _read_app_properties(self, file_path, module, sub_module,  code_bucket=None):
        """
        :param file_path: File path of the json
        :param module: Module name
        :param sub_module: Sub Module name
        :param code_bucket: Code bucket name
        """

        obj = self._read_json_file(file_path, code_bucket=code_bucket)
        return Agg1JsonProcessor.process_app_properties(module, sub_module, obj)

    def _read_conn_properties(self, file_path, code_bucket=None):
        """
        :param file_path: File path of the conn properties
        :param code_bucket: Code bucket name
        """

        obj = self._read_json_file(file_path, code_bucket=code_bucket)
        return Agg1JsonProcessor.process_conn_properties(obj, 'redshift')

    def _get_schema_columns(self, file_path, code_bucket=None):
        obj = self._read_json_file(file_path, code_bucket=code_bucket)
        data = Agg1JsonProcessor.json_parser(obj)
        return [elem['column_name'] for elem in data]

    def read_fact_table(self):
        """
        Read the fact table data
        """

        full_tbl_name = '.'.join([self.config['input']['database'], self.config['input']['normal_cdr_table']])
        src_cols = ','.join(self.src_cols)
        self.logger.info('Reading fact table {tbl} from {start} to {end}'.format(
            tbl=full_tbl_name, start=self.start, end=self.end
        ))
        self.logger.info("Reading columns {cols}.".format(cols=src_cols))
        if self.creation_date_range:
            query = "SELECT {cols} FROM {table} WHERE {date_daily_col} IN " \
                    "(SELECT DISTINCT {date_daily_col} " \
                    "FROM {table} WHERE {created_date_col} >= '{start}' AND {created_date_col} <= '{end}')"\
                .format(cols=src_cols, table=full_tbl_name, start=self.creation_date_range[0],
                        end=self.creation_date_range[1], created_date_col=self.config['input']['created_date_col'],
                        date_daily_col=self.config['input']['date_daily_col']
                        )
        else:
            if self.fmt.startswith('%Y%m%d'):
                query = "SELECT {cols} FROM {table} WHERE {date_daily_col} >= {start} AND {date_daily_col} <= {end}".format(
                        cols=src_cols, table=full_tbl_name, start=self.start, end=self.end,
                        date_daily_col=self.config['input']['date_daily_col']
                        )
            elif self.fmt.startswith('%Y%m'):
                query = "SELECT {cols} FROM {table} WHERE {date_monthly_col} >= {start} AND {date_monthly_col} <= {end}".format(
                    cols=src_cols, table=full_tbl_name, start=self.start, end=self.end,
                    date_monthly_col=self.config['input']['date_monthly_col']
                )
            elif self.fmt.startswith('%Y'):
                query = "SELECT {cols} FROM {table} WHERE {date_monthly_col} >= {start} AND {date_monthly_col} <= {end}".format(
                    cols=src_cols, table=full_tbl_name, start=self.start + '01', end=self.end + '12',
                    date_monthly_col=self.config['input']['date_monthly_col']
                )
            else:
                query = "SELECT {cols} FROM {table} WHERE {date_hour_col} >= {start} AND {date_hour_col} <= {end}".format(
                    cols=src_cols, table=full_tbl_name, start=self.start, end=self.end,
                    date_hour_col=self.config['input']['date_hour_col']
                )

        self.logger.info("Executing: {query}".format(query=query))

        df = self.rs_utils.read_from_redshift_with_query(query)
        return df

    def read_agg_tables(self):
        """
        Reads the aggregated tables
        """

        self.logger.info("Reading aggregated tables...")

        hourly_df = None
        daily_df = None
        monthly_df = None
        yearly_df = None

        # Get the data from aggregated table
        if self.fmt.startswith('%Y%m%d') or self.creation_date_range:
            self.logger.info("Getting data from hourly table.")

            # get hourly data from agg table
            full_hourly_tbl_name = '.'.join(
                [self.config['output']['database'], self.config['output']['hourly_table']])
            query = "SELECT {cols} FROM {table} WHERE {gprs_conn_date} >= {start} AND {gprs_conn_date} <= {end}".format(
                cols=','.join(self.hourly_cols), table=full_hourly_tbl_name, start=self.start, end=self.end,
                gprs_conn_date='data_connection_dt_num'
            )
            _hourly_cols = self.hourly_cols
            hourly_df = self.rs_utils.read_from_redshift_with_query(query).select(_hourly_cols)

            self.logger.info("Getting data from daily table.")

            # get daily data from agg table
            full_daily_tbl_name = '.'.join(
                [self.config['output']['database'], self.config['output']['daily_table']])
            query = "SELECT {cols} FROM {table} WHERE {gprs_conn_date} >= {start} AND {gprs_conn_date} <= {end}".format(
                cols=','.join(self.daily_cols), table=full_daily_tbl_name, start=self.start, end=self.end,
                gprs_conn_date='data_connection_dt_num'
            )
            _daily_cols = self.daily_cols
            daily_df = self.rs_utils.read_from_redshift_with_query(query).select(_daily_cols)

        if self.fmt.startswith('%Y%m') or self.creation_date_range:

            self.logger.info("Getting data from monthly table.")

            # get monthly data from agg table
            full_monthly_tbl_name = '.'.join(
                [self.config['output']['database'], self.config['output']['monthly_table']])
            query = "SELECT {cols} FROM {table} WHERE {gprs_conn_month} >= {start} AND {gprs_conn_month} <= {end}".format(
                cols=','.join(self.monthly_cols), table=full_monthly_tbl_name, start=self.start[:6], end=self.end[:6],
                gprs_conn_month='data_connection_month'
            )
            _monthly_cols = self.monthly_cols
            monthly_df = self.rs_utils.read_from_redshift_with_query(query).select(_monthly_cols)

        if self.fmt.startswith('%Y') or self.creation_date_range:

            self.logger.info("Getting data from yearly table.")

            # get yearly data from agg table
            full_yearly_tbl_name = '.'.join([self.config['output']['database'], self.config['output']['yearly_table']])
            query = "SELECT {cols} FROM {table} WHERE {gprs_conn_year} >= {start} AND {gprs_conn_year} <= {end}".format(
                cols=','.join(self.yearly_cols), table=full_yearly_tbl_name, start=self.start[:4],
                end=self.end[:4],
                gprs_conn_year='data_connection_year'
            )
            _yearly_cols = self.yearly_cols
            yearly_df = self.rs_utils.read_from_redshift_with_query(query).select(_yearly_cols)

        return hourly_df, daily_df, monthly_df, yearly_df

    def perform_distinct_aggregation(self, df: DataFrame, hourly_df=None, daily_df=None, monthly_df=None, yearly_df=None):
        """
        Perform the aggregation on the normal cdr records
        """

        # Register table
        df.createOrReplaceTempView(self.spark_table_name)
        df_distinct_groupby_hourly = None
        df_distinct_groupby_daily = None
        df_distinct_groupby_monthly = None
        df_distinct_groupby_yearly = None

        if self.fmt.startswith('%Y%m%d') or self.creation_date_range:

            df_distinct_groupby_hourly = self.spark.sql(rrbs_gprs_conn_hourly.format(table=self.spark_table_name))
            if df_distinct_groupby_hourly and hourly_df:
                df_distinct_groupby_hourly = df_distinct_groupby_hourly.subtract(hourly_df)

            df_distinct_groupby_daily = self.spark.sql(rrbs_gprs_conn_daily.format(table=self.spark_table_name))
            if df_distinct_groupby_daily and daily_df:
                df_distinct_groupby_daily = df_distinct_groupby_daily.subtract(daily_df)

        if self.fmt.startswith('%Y%m') or self.creation_date_range:
            df_distinct_groupby_monthly = self.spark.sql(rrbs_gprs_conn_monthly.format(table=self.spark_table_name))
            if monthly_df and df_distinct_groupby_monthly:
                df_distinct_groupby_monthly = df_distinct_groupby_monthly.subtract(monthly_df)

        if self.fmt.startswith('%Y') or self.creation_date_range:
            df_distinct_groupby_yearly = self.spark.sql(rrbs_gprs_conn_yearly.format(table=self.spark_table_name))
            if yearly_df and df_distinct_groupby_yearly:
                df_distinct_groupby_yearly = df_distinct_groupby_yearly.subtract(yearly_df)

        return df_distinct_groupby_hourly, \
               df_distinct_groupby_daily,\
               df_distinct_groupby_monthly,\
               df_distinct_groupby_yearly

    def write_aggregation_data(self, frequency, all_aggregation):
        self.logger.info("In write_aggregation_data, writing aggregation for frequency: {freq}".format(freq=frequency))
        for freq, aggregated_df in zip(frequency, all_aggregation):
            if aggregated_df and aggregated_df.count() > 0:
                if freq == 'hourly':
                    self.logger.info("Writing hourly aggregation data.")
                    final_cols = self.hourly_cols
                    temp_table = '.'.join([self.config['output']['database'], self.config['output']['hourly_temp_table']])
                    final_table = '.'.join([self.config['output']['database'], self.config['output']['hourly_table']])

                elif freq == 'daily':
                    self.logger.info("Writing daily aggregation data.")
                    final_cols = self.daily_cols
                    temp_table = '.'.join([self.config['output']['database'], self.config['output']['daily_temp_table']])
                    final_table = '.'.join([self.config['output']['database'], self.config['output']['daily_table']])

                elif freq == 'monthly':
                    self.logger.info("Writing monthly aggregation data.")
                    final_cols = self.monthly_cols
                    temp_table = '.'.join([self.config['output']['database'], self.config['output']['monthly_temp_table']])
                    final_table = '.'.join([self.config['output']['database'], self.config['output']['monthly_table']])

                elif freq == 'yearly':
                    self.logger.info("Writing yearly aggregation data.")
                    final_cols = self.yearly_cols
                    temp_table = '.'.join([self.config['output']['database'], self.config['output']['yearly_temp_table']])
                    final_table = '.'.join([self.config['output']['database'], self.config['output']['yearly_table']])

                else:
                    self.logger.error("Frequency of type {freq} is not valid.".format(freq=freq))
                    raise Exception("Frequency of type {freq} is not valid.".format(freq=freq))

                # Write to redshift
                self.rs_utils.write_to_redshift_with_temp_table(
                    temp_table=temp_table,
                    final_table=final_table,
                    cols=final_cols,
                    df=aggregated_df
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
    sub_module = args.get('sub_module')

    logger.info("Starting aggregation job for {module}.{submodule}.".format(module=module,
                                                                            submodule=sub_module))

    aggregator = Aggregation(spark, module, sub_module, creation_date_range, start_date, end_date, fmt, conn_file_path,
                             config_file_path, code_bucket, logger)

    return aggregator


def start_execution(args):
    """
    :param args: All the arguments
    """

    app_name = args.get('module') + '-' + args.get('sub_module')
    spark_session_build = Agg1SparkSession(args.get('master'), app_name).spark_session_build()
    spark = spark_session_build.get("sparkSession")
    logger = spark_session_build.get("logger")

    aggregator = get_aggregation_instance(spark, logger, args)

    aggregator.logger.info("Starting execution.")

    # Read data
    df = aggregator.read_fact_table()

    if df.count() > 0:
        # Read aggregated tables
        hourly_table, daily_table, monthly_df, yearly_df = aggregator.read_agg_tables()

        # Perform aggregation
        all_aggregation = aggregator.perform_distinct_aggregation(df, hourly_table, daily_table, monthly_df, yearly_df)
        frequency = ['hourly', 'daily', 'monthly', 'yearly']

        # Write to Redshift
        aggregator.write_aggregation_data(frequency, all_aggregation)

        aggregator.logger.info("Finishing execution.")

    else:
        aggregator.logger.info('No input data available')

