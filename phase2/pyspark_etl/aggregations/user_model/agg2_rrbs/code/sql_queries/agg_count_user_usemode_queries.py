"""
Sql queries for aggregation of count user usemode
S.No: 2.6
"""

hourly_query = """
SELECT 
use_mode
, count(cli) as ct
, user_type
, call_date_hour
, call_date_num
, call_date_month
, call_date_year
, call_date_dt
, 1 as is_recent
FROM 
-- Voice
(SELECT DISTINCT
    charged_party_number as cli
, case when a.bundle_code>1 then 'bundle'
    when a.bundle_code<=1 then 'payg'
        else 'unknown' end as user_type
    ,'voice' as use_mode
    ,call_date_hour
    ,call_date_num
    ,call_date_month
    ,call_date_year
    ,call_date_dt
    FROM
    {voice_table}  a
UNION
-- SMS
SELECT DISTINCT
    cli as cli
, case when a.bundle_code>1 then 'bundle'
    when a.bundle_code<=1 then 'payg'
        else 'unknown' end as user_type
    ,'sms' as use_mode
    ,msg_date_hour 
    ,msg_date_num
    ,msg_date_month
    ,msg_date_year
    ,msg_date_dt
    FROM
    {sms_table}  a
UNION
-- GPRS Termination
SELECT DISTINCT
    msisdn as cli
, case when a.bundle_code>1 then 'bundle'
    when a.bundle_code<=1 then 'payg'
        else 'unknown' end as user_type
    ,'gprs' as use_mode
    ,data_termination_hour
    ,data_termination_dt_num
    ,data_termination_month
    ,data_termination_year
    ,data_termination_dt
    FROM
    {gprs_term_table}  a
UNION
-- GPRS Connection
SELECT DISTINCT
    msisdn as cli
, case when a.bundle_code>1 then 'bundle'
    when a.bundle_code<=1 then 'payg'
        else 'unknown' end as user_type
    ,'gprs' as use_mode
    ,data_connection_hour
    ,data_connection_dt_num
    ,data_connection_month
    ,data_connection_year
    ,data_connection_dt
    FROM
    {gprs_conn_table}  a
    ) A
GROUP BY use_mode
    ,user_type
    ,call_date_hour
    ,call_date_num
    ,call_date_dt
    ,call_date_month
    ,call_date_year
"""

daily_query = """
SELECT 
use_mode
, count(cli) as ct
, user_type
, call_date_num
, call_date_month
, call_date_year
, call_date_dt
, 1 as is_recent
FROM 
-- Voice
(SELECT DISTINCT
    charged_party_number as cli
, case when a.bundle_code>1 then 'bundle'
    when a.bundle_code<=1 then 'payg'
        else 'unknown' end as user_type
    ,'voice' as use_mode
    ,call_date_num
    ,call_date_month
    ,call_date_year
    ,call_date_dt
    FROM
    {voice_table}  a
UNION
-- SMS
SELECT DISTINCT
    cli as cli
, case when a.bundle_code>1 then 'bundle'
    when a.bundle_code<=1 then 'payg'
        else 'unknown' end as user_type
    ,'sms' as use_mode
    ,msg_date_num
    ,msg_date_month
    ,msg_date_year
    ,msg_date_dt
    FROM
    {sms_table}  a
UNION
-- GPRS Termination
SELECT DISTINCT
    msisdn as cli
, case when a.bundle_code>1 then 'bundle'
    when a.bundle_code<=1 then 'payg'
        else 'unknown' end as user_type
    ,'gprs' as use_mode
    ,data_termination_dt_num
    ,data_termination_month
    ,data_termination_year
    ,data_termination_dt
    FROM
    {gprs_term_table}  a
UNION
-- GPRS Connection
SELECT DISTINCT
    msisdn as cli
, case when a.bundle_code>1 then 'bundle'
    when a.bundle_code<=1 then 'payg'
        else 'unknown' end as user_type
    ,'gprs' as use_mode
    ,data_connection_dt_num
    ,data_connection_month
    ,data_connection_year
    ,data_connection_dt
    FROM
    {gprs_conn_table}  a
    ) A
GROUP BY use_mode
    ,user_type
    ,call_date_num
    ,call_date_dt
    ,call_date_month
    ,call_date_year
"""

monthly_query = """
SELECT 
use_mode
, count(cli) as ct
, user_type
, call_date_month
, call_date_year
, 1 as is_recent
FROM 
-- Voice
(SELECT DISTINCT
    charged_party_number as cli
, case when a.bundle_code>1 then 'bundle'
    when a.bundle_code<=1 then 'payg'
        else 'unknown' end as user_type
    ,'voice' as use_mode
    ,call_date_month
    ,call_date_year
    FROM
    {voice_table}  a
UNION
-- SMS
SELECT DISTINCT
    cli as cli
, case when a.bundle_code>1 then 'bundle'
    when a.bundle_code<=1 then 'payg'
        else 'unknown' end as user_type
    ,'sms' as use_mode
    ,msg_date_month
    ,msg_date_year
    FROM
    {sms_table}  a
UNION
-- GPRS Termination
SELECT DISTINCT
    msisdn as cli
, case when a.bundle_code>1 then 'bundle'
    when a.bundle_code<=1 then 'payg'
        else 'unknown' end as user_type
    ,'gprs' as use_mode
    ,data_termination_month
    ,data_termination_year
    FROM
    {gprs_term_table}  a
UNION
-- GPRS Connection
SELECT DISTINCT
    msisdn as cli
, case when a.bundle_code>1 then 'bundle'
    when a.bundle_code<=1 then 'payg'
        else 'unknown' end as user_type
    ,'gprs' as use_mode
    ,data_connection_month
    ,data_connection_year
    FROM
    {gprs_conn_table}  a
    ) A
GROUP BY use_mode
    ,user_type
    ,call_date_month
    ,call_date_year
"""

yearly_query = """
SELECT 
use_mode
, count(cli) as ct
, user_type
, call_date_year
, 1 as is_recent
FROM 
-- Voice
(SELECT DISTINCT
    charged_party_number as cli
, case when a.bundle_code>1 then 'bundle'
    when a.bundle_code<=1 then 'payg'
        else 'unknown' end as user_type
    ,'voice' as use_mode
    ,call_date_year
    FROM
    {voice_table}  a
UNION
-- SMS
SELECT DISTINCT
    cli as cli
, case when a.bundle_code>1 then 'bundle'
    when a.bundle_code<=1 then 'payg'
        else 'unknown' end as user_type
    ,'sms' as use_mode
    ,msg_date_year
    FROM
    {sms_table}  a
UNION
-- GPRS Termination
SELECT DISTINCT
    msisdn as cli
, case when a.bundle_code>1 then 'bundle'
    when a.bundle_code<=1 then 'payg'
        else 'unknown' end as user_type
    ,'gprs' as use_mode
    ,data_termination_year
    FROM
    {gprs_term_table}  a
UNION
-- GPRS Connection
SELECT DISTINCT
    msisdn as cli
, case when a.bundle_code>1 then 'bundle'
    when a.bundle_code<=1 then 'payg'
        else 'unknown' end as user_type
    ,'gprs' as use_mode
    ,data_connection_year
    FROM
    {gprs_conn_table}  a
    ) A
GROUP BY use_mode
    ,user_type
    ,call_date_year
"""