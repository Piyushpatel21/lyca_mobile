"""
Contains SQL queries for aggregation
"""

rrbs_sms_hourly = """
SELECT DISTINCT 
    cli
    , case when bundle_code>1 then 'Bundle_use' else 'Payg' end as user_type
    , bundle_code
    , call_type
    , cdr_types
    , destination_zone_code
    , destination_area_code
    , destination_zone_name
    , network_id
    , msg_date_hour
    , msg_date_dt
    , msg_date_num
    , msg_date_month
    , CAST(LEFT(msg_date_month, 4) as INT) as msg_date_year
     from {table}
"""

rrbs_sms_daily = """
SELECT DISTINCT 
    cli
    , case when bundle_code>1 then 'Bundle_use' else 'Payg' end as user_type
    , bundle_code
    , call_type
    , cdr_types
    , destination_zone_code
    , destination_area_code
    , destination_zone_name
    , network_id
    , msg_date_dt
    , msg_date_num
    , msg_date_month
    , CAST(LEFT(msg_date_month, 4) as INT) as msg_date_year
     from {table}
"""

rrbs_sms_monthly = """
SELECT DISTINCT 
    cli
    , case when bundle_code>1 then 'Bundle_use' else 'Payg' end as user_type
    , bundle_code
    , call_type
    , cdr_types
    , destination_zone_code
    , destination_area_code
    , destination_zone_name
    , network_id
    , msg_date_month
    , CAST(LEFT(msg_date_month, 4) as INT) as msg_date_year
     from {table}
"""

rrbs_sms_yearly = """
SELECT DISTINCT 
    cli
    , case when bundle_code>1 then 'Bundle_use' else 'Payg' end as user_type
    , bundle_code
    , call_type
    , cdr_types
    , destination_zone_code
    , destination_area_code
    , destination_zone_name
    , network_id
    , CAST(LEFT(msg_date_month, 4) as INT) as msg_date_year
     from {table}
"""