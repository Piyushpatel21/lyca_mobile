"""
Sql queries for aggregation of count calltype user queries
S.No 2.3
"""

hourly_query = """
-- Voice
SELECT
    COUNT(DISTINCT(charged_party_number)) ct
    ,'voice' as use_mode
    ,b.sub_category as call_type
    ,call_date_hour
    ,call_date_num
    ,call_date_month
    ,call_date_year
    ,call_date_dt
    ,1 as is_recent
    FROM
            {voice_table}  a
left join   {dim_voice_call_category}  b
on
    (a.call_type = b.call_type and a.roam_flag = b.roam_flag and a.call_type = b.call_type and a.call_feature = b.call_feature
    OR A.roam_flag = B.roam_flag AND A.call_type = B.call_type AND '-1' = B.call_feature)
group by
    use_mode
    ,b.sub_category
    ,call_date_year
    ,call_date_month
    ,call_date_num
    ,call_date_dt
    ,call_date_hour
union
-- SMS
SELECT
    count(cli) ct
     ,'sms' as use_mode
     ,b.sub_category as call_type
    ,msg_date_hour 
    ,msg_date_num
    ,msg_date_month
    ,msg_date_year
    ,msg_date_dt
    ,1 as is_recent
    FROM
            {sms_table}  a
left join   {dim_sms_call_category}  b
on
    (a.call_type = b.call_type and a.roam_flag = b.roam_flag and a.call_type = b.call_type)
group by
    use_mode
    ,b.sub_category
    ,msg_date_year
    ,msg_date_month
    ,msg_date_num
    ,msg_date_dt
    ,msg_date_hour
union
-- GPRS Termination
select
    count(msisdn) ct
     ,'gprs' as use_mode
     ,b.roam_flag_val as call_type
     ,data_termination_hour
     ,data_termination_dt_num
    ,data_termination_month
    ,data_termination_year
    ,data_termination_dt
    ,1 as is_recent
    FROM
            {gprs_term_table}  a
left join   {dim_data_roamflag} b
on
    (a.roam_flag = b.roam_flag_id)
group by
    use_mode
    ,b.roam_flag_val
    ,data_termination_year
    ,data_termination_month
    ,data_termination_dt_num
    ,data_termination_dt
    ,data_termination_hour
union
-- GPRS Connection
SELECT
    count(msisdn) ct
     ,'gprs' as use_mode
     ,b.roam_flag_val as call_type
     ,data_connection_hour
     ,data_connection_dt_num
    ,data_connection_month
    ,data_connection_year
    ,data_connection_dt
    ,1 as is_recent
    from
            {gprs_conn_table}  a
left join   {dim_data_roamflag} b
on
    (a.roam_flag = b.roam_flag_id)
group by
    use_mode
    ,b.roam_flag_val
    ,data_connection_year
    ,data_connection_month
    ,data_connection_dt_num
    ,data_connection_dt
    ,data_connection_hour
"""

daily_query = """
-- Voice
SELECT
    COUNT(DISTINCT(charged_party_number)) ct
    ,'voice' as use_mode
    ,b.sub_category as call_type
    ,call_date_num
    ,call_date_month
    ,call_date_year
    ,call_date_dt
    ,1 as is_recent
    FROM
            {voice_table}  a
left join   {dim_voice_call_category}  b
on
    (a.call_type = b.call_type and a.roam_flag = b.roam_flag and a.call_type = b.call_type and a.call_feature = b.call_feature
    OR A.roam_flag = B.roam_flag AND A.call_type = B.call_type AND '-1' = B.call_feature)
group by
    use_mode
    ,b.sub_category
    ,call_date_year
    ,call_date_month
    ,call_date_num
    ,call_date_dt
union
-- SMS
SELECT
    count(cli) ct
     ,'sms' as use_mode
     ,b.sub_category as call_type
    ,msg_date_num
    ,msg_date_month
    ,msg_date_year
    ,msg_date_dt
    ,1 as is_recent
    FROM
            {sms_table}  a
left join   {dim_sms_call_category}  b
on
    (a.call_type = b.call_type and a.roam_flag = b.roam_flag and a.call_type = b.call_type)
group by
    use_mode
    ,b.sub_category
    ,msg_date_year
    ,msg_date_month
    ,msg_date_num
    ,msg_date_dt
union
-- GPRS Termination
select
    count(msisdn) ct
     ,'gprs' as use_mode
     ,b.roam_flag_val as call_type
     ,data_termination_dt_num
    ,data_termination_month
    ,data_termination_year
    ,data_termination_dt
    ,1 as is_recent
    FROM
            {gprs_term_table}  a
left join   {dim_data_roamflag} b
on
    (a.roam_flag = b.roam_flag_id)
group by
    use_mode
    ,b.roam_flag_val
    ,data_termination_year
    ,data_termination_month
    ,data_termination_dt_num
    ,data_termination_dt
union
-- GPRS Connection
SELECT
    count(msisdn) ct
     ,'gprs' as use_mode
     ,b.roam_flag_val as call_type
     ,data_connection_dt_num
    ,data_connection_month
    ,data_connection_year
    ,data_connection_dt
    ,1 as is_recent
    from
            {gprs_conn_table}  a
left join   {dim_data_roamflag} b
on
    (a.roam_flag = b.roam_flag_id)
group by
    use_mode
    ,b.roam_flag_val
    ,data_connection_year
    ,data_connection_month
    ,data_connection_dt_num
    ,data_connection_dt
"""

monthly_query = """
-- Voice
SELECT
    COUNT(DISTINCT(charged_party_number)) ct
    ,'voice' as use_mode
    ,b.sub_category as call_type
    ,call_date_month
    ,call_date_year
    ,1 as is_recent
    FROM
            {voice_table}  a
left join   {dim_voice_call_category}  b
on
    (a.call_type = b.call_type and a.roam_flag = b.roam_flag and a.call_type = b.call_type and a.call_feature = b.call_feature
    OR A.roam_flag = B.roam_flag AND A.call_type = B.call_type AND '-1' = B.call_feature)
group by
    use_mode
    ,b.sub_category
    ,call_date_year
    ,call_date_month
union
-- SMS
SELECT
    count(cli) ct
     ,'sms' as use_mode
     ,b.sub_category as call_type
    ,msg_date_month
    ,msg_date_year
    ,1 as is_recent
    FROM
            {sms_table}  a
left join   {dim_sms_call_category}  b
on
    (a.call_type = b.call_type and a.roam_flag = b.roam_flag and a.call_type = b.call_type)
group by
    use_mode
    ,b.sub_category
    ,msg_date_year
    ,msg_date_month
union
-- GPRS Termination
select
    count(msisdn) ct
     ,'gprs' as use_mode
     ,b.roam_flag_val as call_type
    ,data_termination_month
    ,data_termination_year
    ,1 as is_recent
    FROM
            {gprs_term_table}  a
left join   {dim_data_roamflag} b
on
    (a.roam_flag = b.roam_flag_id)
group by
    use_mode
    ,b.roam_flag_val
    ,data_termination_year
    ,data_termination_month
union
-- GPRS Connection
SELECT
    count(msisdn) ct
     ,'gprs' as use_mode
     ,b.roam_flag_val as call_type
    ,data_connection_month
    ,data_connection_year
    ,1 as is_recent
    from
            {gprs_conn_table}  a
left join   {dim_data_roamflag} b
on
    (a.roam_flag = b.roam_flag_id)
group by
    use_mode
    ,b.roam_flag_val
    ,data_connection_year
    ,data_connection_month
"""

yearly_query = """
-- Voice
SELECT
    COUNT(DISTINCT(charged_party_number)) ct
    ,'voice' as use_mode
    ,b.sub_category as call_type
    ,call_date_year
    ,1 as is_recent
    FROM
            {voice_table}  a
left join   {dim_voice_call_category}  b
on
    (a.call_type = b.call_type and a.roam_flag = b.roam_flag and a.call_type = b.call_type and a.call_feature = b.call_feature
    OR A.roam_flag = B.roam_flag AND A.call_type = B.call_type AND '-1' = B.call_feature)
group by
    use_mode
    ,b.sub_category
    ,call_date_year
union
-- SMS
SELECT
    count(cli) ct
     ,'sms' as use_mode
     ,b.sub_category as call_type
    ,msg_date_year
    ,1 as is_recent
    FROM
            {sms_table}  a
left join   {dim_sms_call_category}  b
on
    (a.call_type = b.call_type and a.roam_flag = b.roam_flag and a.call_type = b.call_type)
group by
    use_mode
    ,b.sub_category
    ,msg_date_year
union
-- GPRS Termination
select
    count(msisdn) ct
     ,'gprs' as use_mode
     ,b.roam_flag_val as call_type
    ,data_termination_year
    ,1 as is_recent
    FROM
            {gprs_term_table}  a
left join   {dim_data_roamflag} b
on
    (a.roam_flag = b.roam_flag_id)
group by
    use_mode
    ,b.roam_flag_val
    ,data_termination_year
union
-- GPRS Connection
SELECT
    count(msisdn) ct
     ,'gprs' as use_mode
     ,b.roam_flag_val as call_type
    ,data_connection_year
    ,1 as is_recent
    from
            {gprs_conn_table}  a
left join   {dim_data_roamflag} b
on
    (a.roam_flag = b.roam_flag_id)
group by
    use_mode
    ,b.roam_flag_val
    ,data_connection_year
"""