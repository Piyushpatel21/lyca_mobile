"""
Sql queries for aggregation of count usemode
"""

hourly_query = """
SELECT 
    use_mode
    , COUNT(cli) as user_count
    , call_date_hour
    , call_date_num
    , call_date_month
    , call_date_year
    , 1 as is_recent
FROM (
    SELECT DISTINCT
        'voice' as use_mode
        , charged_party_number as cli
        , call_date_hour
        , call_date_num 
        , call_date_month
        , call_date_year
    FROM {voice_table}
    UNION
    SELECT DISTINCT
        'sms' as use_mode
        , cli as cli
        , msg_date_hour 
        , msg_date_num
        , msg_date_month
        , msg_date_year
    FROM {sms_table} 
    UNION
    SELECT DISTINCT
        'gprs_conn' as use_mode
        , msisdn as cli
        , data_connection_hour
        , data_connection_dt_num
        , data_connection_month
        , data_connection_year
    FROM {gprs_conn_table}
    UNION
    SELECT DISTINCT
        'gprs_conn' as use_mode
        , msisdn as cli
        , data_termination_hour
        , data_termination_dt_num
        , data_termination_month
        , data_termination_year
    FROM {gprs_term_table}
) A
group by use_mode
,call_date_hour
,call_date_num
,call_date_month
,call_date_year
"""

daily_query = """
SELECT 
    use_mode
    , COUNT(cli) as user_count
    , call_date_num
    , call_date_month
    , call_date_year
    , 1 as is_recent
FROM (
    SELECT DISTINCT
        'voice' as use_mode
        , charged_party_number as cli
        , call_date_num 
        , call_date_month
        , call_date_year
    FROM {voice_table}
    UNION
    SELECT DISTINCT
        'sms' as use_mode
        , cli as cli
        , msg_date_num
        , msg_date_month
        , msg_date_year
    FROM {sms_table} 
    UNION
    SELECT DISTINCT
        'gprs_conn' as use_mode
        , msisdn as cli
        , data_connection_dt_num
        , data_connection_month
        , data_connection_year
    FROM {gprs_conn_table}
    UNION
    SELECT DISTINCT
        'gprs_conn' as use_mode
        , msisdn as cli
        , data_termination_dt_num
        , data_termination_month
        , data_termination_year
    FROM {gprs_term_table}
) A
group by use_mode
,call_date_num
,call_date_month
,call_date_year
"""

monthly_query = """
SELECT 
    use_mode
    , COUNT(cli) as user_count
    , call_date_month
    , call_date_year
    , 1 as is_recent
FROM (
    SELECT DISTINCT
        'voice' as use_mode
        , charged_party_number as cli
        , call_date_month
        , call_date_year
    FROM {voice_table}
    UNION
    SELECT DISTINCT
        'sms' as use_mode
        , cli as cli
        , msg_date_month
        , msg_date_year
    FROM {sms_table} 
    UNION
    SELECT DISTINCT
        'gprs_conn' as use_mode
        , msisdn as cli
        , data_connection_month
        , data_connection_year
    FROM {gprs_conn_table}
    UNION
    SELECT DISTINCT
        'gprs_conn' as use_mode
        , msisdn as cli
        , data_termination_month
        , data_termination_year
    FROM {gprs_term_table}
) A
group by use_mode
,call_date_month
,call_date_year
"""

yearly_query = """
SELECT 
    use_mode
    , COUNT(cli) as user_count
    , call_date_year
    , 1 as is_recent
FROM (
    SELECT DISTINCT
        'voice' as use_mode
        , charged_party_number as cli
        , call_date_year
    FROM {voice_table}
    UNION
    SELECT DISTINCT
        'sms' as use_mode
        , cli as cli
        , msg_date_year
    FROM {sms_table} 
    UNION
    SELECT DISTINCT
        'gprs_conn' as use_mode
        , msisdn as cli
        , data_connection_year
    FROM {gprs_conn_table}
    UNION
    SELECT DISTINCT
        'gprs_conn' as use_mode
        , msisdn as cli
        , data_termination_year
    FROM {gprs_term_table}
) A
group by use_mode
,call_date_year
"""
