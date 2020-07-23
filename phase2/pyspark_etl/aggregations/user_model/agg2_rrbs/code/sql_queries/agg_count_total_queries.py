"""
Sql queries for aggregation of total count
"""

hourly_query = """
SELECT 
    user_type
    , COUNT(DISTINCT cli) as user_count
    , call_date_hour
    , call_date_num
    , call_date_month
    , call_date_year
    , 1 as is_recent
FROM (
    SELECT DISTINCT
        user_type
        , charged_party_number as cli
        , call_date_hour
        , call_date_num 
        , call_date_month
        , call_date_year
    FROM {voice_table}
    UNION
    SELECT DISTINCT
        user_type
        , cli as cli
        , msg_date_hour 
        , msg_date_num
        , msg_date_month
        , msg_date_year
    FROM {sms_table} 
    UNION
    SELECT DISTINCT
        user_type
        , msisdn as cli
        , data_connection_hour
        , data_connection_dt_num
        , data_connection_month
        , data_connection_year
    FROM {gprs_conn_table}
) A
group by user_type
,call_date_hour
,call_date_num
,call_date_month
,call_date_year
"""

daily_query = """
SELECT 
    user_type
    , COUNT(DISTINCT cli) as user_count
    , call_date_num
    , call_date_month
    , call_date_year
    , 1 as is_recent
FROM (
    SELECT DISTINCT
        user_type
        , charged_party_number as cli
        , call_date_num 
        , call_date_month
        , call_date_year
    FROM {voice_table}
    UNION
    SELECT DISTINCT
        user_type
        , cli as cli
        , msg_date_num
        , msg_date_month
        , msg_date_year
    FROM {sms_table} 
    UNION
    SELECT DISTINCT
        user_type
        , msisdn as cli
        , data_connection_dt_num
        , data_connection_month
        , data_connection_year
    FROM {gprs_conn_table}
) A
group by user_type
,call_date_num
,call_date_month
,call_date_year
"""

monthly_query = """
SELECT 
    user_type
    , COUNT(DISTINCT cli) as user_count
    , call_date_month
    , call_date_year
    , 1 as is_recent
FROM (
    SELECT DISTINCT
        user_type
        , charged_party_number as cli
        , call_date_month 
        , call_date_year
    FROM {voice_table}
    UNION
    SELECT DISTINCT
        user_type
        , cli as cli
        , msg_date_month
        , msg_date_year
    FROM {sms_table} 
    UNION
    SELECT DISTINCT
        user_type
        , msisdn as cli
        , data_connection_month
        , data_connection_year
    FROM {gprs_conn_table}
) A
group by user_type
,call_date_month
,call_date_year
"""

yearly_query = """
SELECT 
    user_type
    , COUNT(DISTINCT cli) as user_count
    , call_date_year
    , 1 as is_recent
FROM (
    SELECT DISTINCT
        user_type
        , charged_party_number as cli
        , call_date_year
    FROM {voice_table}
    UNION
    SELECT DISTINCT
        user_type
        , cli as cli
        , msg_date_year
    FROM {sms_table} 
    UNION
    SELECT DISTINCT
        user_type
        , msisdn as cli
        , data_connection_year
    FROM {gprs_conn_table}
) A
group by user_type
,call_date_year
"""