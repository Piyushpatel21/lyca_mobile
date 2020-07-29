"""
Contains SQL queries for aggregation
"""


rrbs_topup_monthly = """
SELECT DISTINCT 
    msisdn
    , case when bundle_code>1 then 'Bundle_use' else 'Payg' end as user_type
    , bundle_code
    , imsi
    , bundle_name
    , network_id
    , cdr_dt_month
    , CAST(LEFT(cdr_dt_month, 4) as INT) as cdr_dt_year
     from {table}
"""