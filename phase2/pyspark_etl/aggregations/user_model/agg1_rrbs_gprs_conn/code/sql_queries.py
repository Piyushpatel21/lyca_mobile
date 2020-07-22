"""
Contains SQL queries for aggregation
"""

rrbs_gprs_conn_hourly = """
SELECT DISTINCT 
    msisdn
    , case when bundle_code>1 then 'Bundle_use' else 'Payg' end as user_type
    , bundle_code
    , tariffplan_id
    , cdr_type
    , network_id
    , data_connection_hour
    , data_connection_dt
    , data_connection_dt_num
    , data_connection_month
    , CAST(LEFT(data_connection_month, 4) as INT) as data_connection_year
     from {table}
"""

rrbs_gprs_conn_daily = """
SELECT DISTINCT 
    msisdn
    , case when bundle_code>1 then 'Bundle_use' else 'Payg' end as user_type
    , bundle_code
    , tariffplan_id
    , cdr_type
    , network_id
    , data_connection_dt
    , data_connection_dt_num
    , data_connection_month
    , CAST(LEFT(data_connection_month, 4) as INT) as data_connection_year
     from {table}
"""

rrbs_gprs_conn_monthly = """
SELECT DISTINCT 
    msisdn
    , case when bundle_code>1 then 'Bundle_use' else 'Payg' end as user_type
    , bundle_code
    , tariffplan_id
    , cdr_type
    , network_id
    , data_connection_month
    , CAST(LEFT(data_connection_month, 4) as INT) as data_connection_year
     from {table}
"""

rrbs_gprs_conn_yearly = """
SELECT DISTINCT 
    msisdn
    , case when bundle_code>1 then 'Bundle_use' else 'Payg' end as user_type
    , bundle_code
    , tariffplan_id
    , cdr_type
    , network_id
    , CAST(LEFT(data_connection_month, 4) as INT) as data_connection_year
     from {table}
"""