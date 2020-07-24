"""
Contains SQL queries for aggregation
"""

rrbs_voice_hourly = """
SELECT DISTINCT 
    charged_party_number
    , case when bundle_code>1 then 'Bundle_use' else 'Payg' end as user_type
    , bundle_code
    , tariffplan_id
    , call_type
    , voice_call_cdr
    , destination_zone
    , destination_area_code
    , destinationzone_name as destination_zone_name
    , roam_flag
    , network_id
    , call_feature
    , call_date_hour
    , call_date_dt
    , call_date_num
    , call_date_month
    , CAST(LEFT(call_date_month, 4) as INT) as call_date_year
     from {table}
"""

rrbs_voice_daily = """
SELECT DISTINCT 
    charged_party_number
    , case when bundle_code>1 then 'Bundle_use' else 'Payg' end as user_type
    , bundle_code
    , tariffplan_id
    , call_type
    , voice_call_cdr
    , destination_zone
    , destination_area_code
    , destinationzone_name as destination_zone_name
    , roam_flag
    , network_id
    , call_feature
    , call_date_dt
    , call_date_num
    , call_date_month
    , CAST(LEFT(call_date_month, 4) as INT) as call_date_year
    from {table}
"""

rrbs_voice_monthly = """
SELECT DISTINCT 
    charged_party_number
    , case when bundle_code>1 then 'Bundle_use' else 'Payg' end as user_type
    , bundle_code
    , tariffplan_id
    , call_type
    , voice_call_cdr
    , destination_zone
    , destination_area_code
    , destinationzone_name as destination_zone_name
    , roam_flag
    , network_id
    , call_feature
    , call_date_month
    , CAST(LEFT(call_date_month, 4) as INT) as call_date_year
     from {table}
"""

rrbs_voice_yearly = """
SELECT DISTINCT 
    charged_party_number
    , case when bundle_code>1 then 'Bundle_use' else 'Payg' end as user_type
    , bundle_code
    , tariffplan_id
    , call_type
    , voice_call_cdr
    , destination_zone
    , destination_area_code
    , destinationzone_name as destination_zone_name
    , roam_flag
    , network_id
    , call_feature
    , CAST(LEFT(call_date_month, 4) as INT) as call_date_year
    from {table}
"""