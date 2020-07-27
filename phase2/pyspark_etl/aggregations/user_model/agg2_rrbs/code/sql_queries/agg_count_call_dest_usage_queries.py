"""
Sql queries for aggregation of call user usage queries
"""

hourly_query = """
----------------------------voice---------------------
SELECT
    COUNT(DISTINCT charged_party_number) ct
, CASE WHEN a.bundle_code>1 then 'bundle'
    when a.bundle_code<=1 then 'payg'
        else 'unknown' end as user_type
    ,'voice' as use_mode
    , b.country_code
    , b.country_name
    , call_date_hour
    , call_date_num
    , call_date_month
    , call_date_year
    , call_date_dt
    , 1 as is_recent
    FROM
        {voice_table}  a
LEFT JOIN {dim_destination_master}  b
on
    left(trim(a.destination_zone_name),3) = trim(b.country_code)
GROUP BY
CASE WHEN a.bundle_code>1 then 'bundle'
    when a.bundle_code<=1 then 'payg'
        else 'unknown' end
    , use_mode
    , b.country_code
    , b.country_name
    , call_date_year
    , call_date_month
    , call_date_num
    , call_date_dt
    , call_date_hour
union all
-----------------------------------sms----------------------------
SELECT
    COUNT( DISTINCT cli) ct
, CASE WHEN a.bundle_code>1 then 'bundle'
    when a.bundle_code<=1 then 'payg'
        else 'unknown' end as user_type
    ,'sms' as use_mode
    , b.country_code
    , b.country_name
    , msg_date_hour
    , msg_date_num
    , msg_date_month
    , msg_date_year
    , msg_date_dt
    , 1 as is_recent
    FROM
            {sms_table} a
left join   {dim_destination_master}  b
on
    left(trim(a.destination_zone_name),3) = trim(b.country_code)
GROUP BY
CASE WHEN a.bundle_code>1 then 'bundle'
    when a.bundle_code<=1 then 'payg'
        else 'unknown' end
    , use_mode
    , b.country_code
    , b.country_name
    , msg_date_year
    , msg_date_month
    , msg_date_num
    , msg_date_dt
    , msg_date_hour
"""

daily_query = """
----------------------------voice---------------------
SELECT
    COUNT(DISTINCT charged_party_number) ct
, CASE WHEN a.bundle_code>1 then 'bundle'
    when a.bundle_code<=1 then 'payg'
        else 'unknown' end as user_type
    ,'voice' as use_mode
    , b.country_code
    , b.country_name
    , call_date_num
    , call_date_month
    , call_date_year
    , call_date_dt
    , 1 as is_recent
    FROM
        {voice_table}  a
LEFT JOIN {dim_destination_master}  b
on
    left(trim(a.destination_zone_name),3) = trim(b.country_code)
GROUP BY
CASE WHEN a.bundle_code>1 then 'bundle'
    when a.bundle_code<=1 then 'payg'
        else 'unknown' end
    , use_mode
    , b.country_code
    , b.country_name
    , call_date_year
    , call_date_month
    , call_date_num
    , call_date_dt
union all
-----------------------------------sms----------------------------
SELECT
    COUNT( DISTINCT cli) ct
, CASE WHEN a.bundle_code>1 then 'bundle'
    when a.bundle_code<=1 then 'payg'
        else 'unknown' end as user_type
    ,'sms' as use_mode
    , b.country_code
    , b.country_name
    , msg_date_num
    , msg_date_month
    , msg_date_year
    , msg_date_dt
    , 1 as is_recent
    FROM
            {sms_table} a
left join   {dim_destination_master}  b
on
    left(trim(a.destination_zone_name),3) = trim(b.country_code)
GROUP BY
CASE WHEN a.bundle_code>1 then 'bundle'
    when a.bundle_code<=1 then 'payg'
        else 'unknown' end
    , use_mode
    , b.country_code
    , b.country_name
    , msg_date_year
    , msg_date_month
    , msg_date_num
    , msg_date_dt
"""

monthly_query = """
----------------------------voice---------------------
SELECT
    COUNT(DISTINCT charged_party_number) ct
, CASE WHEN a.bundle_code>1 then 'bundle'
    when a.bundle_code<=1 then 'payg'
        else 'unknown' end as user_type
    ,'voice' as use_mode
    , b.country_code
    , b.country_name
    , call_date_month
    , call_date_year
    , 1 as is_recent
    FROM
        {voice_table}  a
LEFT JOIN {dim_destination_master}  b
on
    left(trim(a.destination_zone_name),3) = trim(b.country_code)
GROUP BY
CASE WHEN a.bundle_code>1 then 'bundle'
    when a.bundle_code<=1 then 'payg'
        else 'unknown' end
    , use_mode
    , b.country_code
    , b.country_name
    , call_date_year
    , call_date_month
union all
-----------------------------------sms----------------------------
SELECT
    COUNT( DISTINCT cli) ct
, CASE WHEN a.bundle_code>1 then 'bundle'
    when a.bundle_code<=1 then 'payg'
        else 'unknown' end as user_type
    ,'sms' as use_mode
    , b.country_code
    , b.country_name
    , msg_date_month
    , msg_date_year
    , 1 as is_recent
    FROM
            {sms_table} a
left join   {dim_destination_master}  b
on
    left(trim(a.destination_zone_name),3) = trim(b.country_code)
GROUP BY
CASE WHEN a.bundle_code>1 then 'bundle'
    when a.bundle_code<=1 then 'payg'
        else 'unknown' end
    , use_mode
    , b.country_code
    , b.country_name
    , msg_date_year
    , msg_date_month
"""

yearly_query = """
----------------------------voice---------------------
SELECT
    COUNT(DISTINCT charged_party_number) ct
, CASE WHEN a.bundle_code>1 then 'bundle'
    when a.bundle_code<=1 then 'payg'
        else 'unknown' end as user_type
    ,'voice' as use_mode
    , b.country_code
    , b.country_name
    , call_date_year
    , 1 as is_recent
    FROM
        {voice_table}  a
LEFT JOIN {dim_destination_master}  b
on
    left(trim(a.destination_zone_name),3) = trim(b.country_code)
GROUP BY
CASE WHEN a.bundle_code>1 then 'bundle'
    when a.bundle_code<=1 then 'payg'
        else 'unknown' end
    , use_mode
    , b.country_code
    , b.country_name
    , call_date_year
union all
-----------------------------------sms----------------------------
SELECT
    COUNT( DISTINCT cli) ct
, CASE WHEN a.bundle_code>1 then 'bundle'
    when a.bundle_code<=1 then 'payg'
        else 'unknown' end as user_type
    ,'sms' as use_mode
    , b.country_code
    , b.country_name
    , msg_date_year
    , 1 as is_recent
    FROM
            {sms_table} a
left join   {dim_destination_master}  b
on
    left(trim(a.destination_zone_name),3) = trim(b.country_code)
GROUP BY
CASE WHEN a.bundle_code>1 then 'bundle'
    when a.bundle_code<=1 then 'payg'
        else 'unknown' end
    , use_mode
    , b.country_code
    , b.country_name
    , msg_date_year
"""