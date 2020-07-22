"""
Contains SQL queries for aggregation
"""

rrbs_gprs_term_hourly = """
SELECT DISTINCT 
    msisdn
    , case when bundle_code>1 then 'Bundle_use' else 'Payg' end as user_type
    , bundle_code
    , tariffplan_id
    , cdr_type
    , network_id
    , data_termination_hour
    , data_termination_dt
    , data_termination_dt_num
    , data_termination_month
    , CAST(LEFT(data_termination_month, 4) as INT) as data_termination_year
     from {table}
"""

rrbs_gprs_term_daily = """
SELECT DISTINCT 
    msisdn
    , case when bundle_code>1 then 'Bundle_use' else 'Payg' end as user_type
    , bundle_code
    , tariffplan_id
    , cdr_type
    , network_id
    , data_termination_dt
    , data_termination_dt_num
    , data_termination_month
    , CAST(LEFT(data_termination_month, 4) as INT) as data_termination_year
     from {table}
"""

rrbs_gprs_term_monthly = """
SELECT DISTINCT 
    msisdn
    , case when bundle_code>1 then 'Bundle_use' else 'Payg' end as user_type
    , bundle_code
    , tariffplan_id
    , cdr_type
    , network_id
    , data_termination_month
    , CAST(LEFT(data_termination_month, 4) as INT) as data_termination_year
     from {table}
"""

rrbs_gprs_term_yearly = """
SELECT DISTINCT 
    msisdn
    , case when bundle_code>1 then 'Bundle_use' else 'Payg' end as user_type
    , bundle_code
    , tariffplan_id
    , cdr_type
    , network_id
    , CAST(LEFT(data_termination_month, 4) as INT) as data_termination_year
     from {table}
"""