"""
Perform Aggregation on Usage model RRBS Voice Data. It performs:
* Read from Redshift
* Calculate Metrics
* Save to Redshift
"""

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import datetime

from code.utils import Agg1SparkSession, Agg1JsonProcessor, Agg1AwsReader
from code.utils import Agg1RedshiftUtils, get_secret, parse_date
from code.sql_queries import *


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
        self.src_cols = ['bundle_code', 'network_id', 'voice_call_cdr', 'call_type', 'call_feature', 'tariffplan_id',
                         'roam_flag', 'roaming_area_code', 'roaming_zone_name', 'roaming_partner_name',
                         'destinationzone_name', 'destination_area_code', 'destination_zone',
                         'call_date_hour', 'call_date_dt', 'call_date_num', 'call_date_month', 'call_duration_min',
                         'chargeable_used_time_min', 'talk_charge']
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

    def read_fact_table(self):
        """
        Read the fact table data
        """

        full_tbl_name = '.'.join([self.config['input']['database'], self.config['input']['normal_cdr_table']])
        vmd_tbl_name = '.'.join([self.config['dim_tables']['database'], self.config['dim_tables']['vmd']])
        src_cols = ','.join(self.src_cols)
        self.logger.info('Reading fact table {tbl} from {start} to {end}'.format(
            tbl=full_tbl_name, start=self.start, end=self.end
        ))
        self.logger.info("Reading columns {cols}.".format(cols=src_cols))
        if self.creation_date_range:
            query = "SELECT {cols} FROM {table} WHERE {date_daily_col} IN " \
                    "(SELECT DISTINCT {date_daily_col} " \
                    "FROM {table} WHERE {created_date_col} >= '{start}' AND {created_date_col} <= '{end}') " \
                    "AND dialed_number NOT IN (SELECT vm_deposit from {vmd_table})"\
                .format(cols=src_cols, table=full_tbl_name, vmd_table=vmd_tbl_name, start=self.creation_date_range[0],
                        end=self.creation_date_range[1], created_date_col=self.config['input']['created_date_col'],
                        date_daily_col=self.config['input']['date_daily_col']
                        )
        else:
            if self.fmt.startswith('%Y%m%d'):
                query = "SELECT {cols} FROM {table} WHERE {date_daily_col} >= {start} AND {date_daily_col} <= {end} " \
                        "AND dialed_number NOT IN (SELECT vm_deposit from {vmd_table})".format(
                        cols=src_cols, table=full_tbl_name, vmd_table=vmd_tbl_name, start=self.start, end=self.end,
                        date_daily_col=self.config['input']['date_daily_col']
                        )
            elif self.fmt.startswith('%Y%m'):
                query = "SELECT {cols} FROM {table} WHERE {date_monthly_col} >= {start} AND {date_monthly_col} <= {end} " \
                        "AND dialed_number NOT IN (SELECT vm_deposit from {vmd_table})".format(
                    cols=src_cols, table=full_tbl_name, vmd_table=vmd_tbl_name, start=self.start, end=self.end,
                    date_monthly_col=self.config['input']['date_monthly_col']
                )
            elif self.fmt.startswith('%Y'):
                query = "SELECT {cols} FROM {table} WHERE {date_monthly_col} >= {start} AND {date_monthly_col} <= {end} " \
                        "AND dialed_number NOT IN (SELECT vm_deposit from {vmd_table})".format(
                    cols=src_cols, table=full_tbl_name, vmd_table=vmd_tbl_name, start=self.start + '01', end=self.end + '12',
                    date_monthly_col=self.config['input']['date_monthly_col']
                )
            else:
                query = "SELECT {cols} FROM {table} WHERE {date_hour_col} >= {start} AND {date_hour_col} <= {end} " \
                        "AND dialed_number NOT IN (SELECT vm_deposit from {vmd_table})".format(
                    cols=src_cols, table=full_tbl_name, vmd_table=vmd_tbl_name, start=self.start, end=self.end,
                    date_hour_col=self.config['input']['date_hour_col']
                )

        self.logger.info("Executing: {query}".format(query=query))

        df = self.rs_utils.read_from_redshift_with_query(query)
        return df

    def read_dim_tables(self):
        """
        Read the dimension tables
        """

        self.logger.info("Reading dimension tables: ")
        dim_destination_master = '.'.join(
            [self.config['dim_tables']['database'], self.config['dim_tables']['dim_destination_master']])

        # get dim_voice_call_category data from agg table
        query = "SELECT * FROM {table}".format(
            table=dim_destination_master
        )
        dim_destination_master_df = self.rs_utils.read_from_redshift_with_query(query)

        return {'dim_destination_master': dim_destination_master_df}

    def perform_aggregation(self, df: DataFrame, dim_tables):
        """
        Perform the aggregation on the normal cdr records
        """

        self.logger.info("Performing aggregation: ")
        # Register table
        df.createOrReplaceTempView(self.spark_table_name)
        df_groupby_hourly = None
        df_groupby_daily = None
        df_groupby_monthly = None
        df_groupby_yearly = None

        if self.fmt.startswith('%Y%m%d') or self.creation_date_range:

            df_groupby_hourly = self.spark.sql(rrbs_voice_hourly.format(fact_table=self.spark_table_name,
                                                                        dim_destination_master=dim_tables[
                                                                               'dim_destination_master']))

            df_groupby_daily = self.spark.sql(rrbs_voice_daily.format(fact_table=self.spark_table_name,
                                                                      dim_destination_master=dim_tables[
                                                                               'dim_destination_master']))

        if self.fmt.startswith('%Y%m') or self.creation_date_range:
            df_groupby_monthly = self.spark.sql(rrbs_voice_monthly.format(fact_table=self.spark_table_name,
                                                                          dim_destination_master=dim_tables[
                                                                               'dim_destination_master']))

        if self.fmt.startswith('%Y') or self.creation_date_range:
            df_groupby_yearly = self.spark.sql(rrbs_voice_yearly.format(fact_table=self.spark_table_name,
                                                                        dim_destination_master=dim_tables[
                                                                               'dim_destination_master']))

        return df_groupby_hourly, \
               df_groupby_daily,\
               df_groupby_monthly,\
               df_groupby_yearly

    def write_aggregation_data(self, frequency, all_aggregation):
        self.logger.info("In write_aggregation_data, writing aggregation for frequency: {freq}".format(freq=frequency))
        for freq, aggregated_df in zip(frequency, all_aggregation):
            if aggregated_df and aggregated_df.count() > 0:
                final_cols = aggregated_df.columns
                if freq == 'hourly':
                    self.logger.info("Writing hourly aggregation data.")
                    temp_table = '.'.join([self.config['output']['database'], self.config['output']['hourly_temp_table']])
                    final_table = '.'.join([self.config['output']['database'], self.config['output']['hourly_table']])
                    suffix_post_query = """
                    UPDATE {table} SET is_recent = 0 
                    WHERE {daily_col} >= {start} AND {daily_col} <= {end} AND
                    created_date_time not in (SELECT MAX(created_date_time) from {table} 
                                               WHERE {daily_col} >= {start} AND {daily_col} <= {end})
                    """.format(table=final_table,
                               daily_col=self.config['output']['date_col'],
                               start=self.start,
                               end=self.end)

                elif freq == 'daily':
                    self.logger.info("Writing daily aggregation data.")
                    temp_table = '.'.join([self.config['output']['database'], self.config['output']['daily_temp_table']])
                    final_table = '.'.join([self.config['output']['database'], self.config['output']['daily_table']])
                    suffix_post_query = """
                    UPDATE {table} SET is_recent = 0 
                    WHERE {daily_col} >= {start} AND {daily_col} <= {end} AND
                    created_date_time not in (SELECT MAX(created_date_time) from {table} 
                                               WHERE {daily_col} >= {start} AND {daily_col} <= {end})
                    """.format(table=final_table,
                               daily_col=self.config['output']['date_col'],
                               start=self.start,
                               end=self.end)

                elif freq == 'monthly':
                    self.logger.info("Writing monthly aggregation data.")
                    temp_table = '.'.join([self.config['output']['database'], self.config['output']['monthly_temp_table']])
                    final_table = '.'.join([self.config['output']['database'], self.config['output']['monthly_table']])
                    suffix_post_query = """
                    UPDATE {table} SET is_recent = 0 
                    WHERE {monthly_col} >= {start} AND {monthly_col} <= {end} AND
                    created_date_time not in (SELECT MAX(created_date_time) from {table} 
                                               WHERE {monthly_col} >= {start} AND {monthly_col} <= {end})
                    """.format(table=final_table,
                               monthly_col=self.config['output']['month_col'],
                               start=self.start[0:6],
                               end=self.end[0:6])

                elif freq == 'yearly':
                    self.logger.info("Writing yearly aggregation data.")
                    temp_table = '.'.join([self.config['output']['database'], self.config['output']['yearly_temp_table']])
                    final_table = '.'.join([self.config['output']['database'], self.config['output']['yearly_table']])
                    suffix_post_query = """
                    UPDATE {table} SET is_recent = 0 
                    WHERE {yearly_col} >= {start} AND {yearly_col} <= {end} AND
                    created_date_time not in (SELECT MAX(created_date_time) from {table} 
                                               WHERE {yearly_col} >= {start} AND {yearly_col} <= {end})
                    """.format(table=final_table,
                               yearly_col=self.config['output']['year_col'],
                               start=self.start[0:4],
                               end=self.end[0:4])

                else:
                    self.logger.error("Frequency of type {freq} is not valid.".format(freq=freq))
                    raise Exception("Frequency of type {freq} is not valid.".format(freq=freq))

                # Write to redshift
                self.rs_utils.write_to_redshift_with_temp_table(
                    temp_table=temp_table,
                    final_table=final_table,
                    cols=final_cols,
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

        # Read dimension tables
        dims = aggregator.read_dim_tables()

        # Register Dimension tables
        dim_tables = {}
        for dim_name, df in dims.items():
            if df:
                df.createOrReplaceTempView(dim_name)
                spark.table(dim_name).persist()
                dim_tables[dim_name] = dim_name
            else:
                dim_tables[dim_name] = None

        # Perform aggregation
        all_aggregation = aggregator.perform_aggregation(df, dim_tables)
        frequency = ['hourly', 'daily', 'monthly', 'yearly']

        # Write to Redshift
        aggregator.write_aggregation_data(frequency, all_aggregation)

        aggregator.logger.info("Finishing execution.")

    else:
        aggregator.logger.info('No input data available')

