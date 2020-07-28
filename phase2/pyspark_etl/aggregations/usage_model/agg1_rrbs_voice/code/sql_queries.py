"""
Contains SQL queries for aggregation
"""

rrbs_voice_hourly = """
SELECT 
      bundle_code
      , user_type
      , network_id
      , voice_call_cdr --cdr_type
      , call_type
      , call_feature
      , tariffplan_id
      , roam_flag
      , roaming_area_code
      , roaming_zone_name
      , roaming_partner_name
      , destinationzone_name as destination_zone_name
      , destination_area_code
      , destination_zone
      , call_date_hour  
      , call_date_dt
      , call_date_num  
      , call_date_month 
      , call_date_year
      , country_name
      , total_call_duration_min
      , total_chargeable_used_time_min
      , total_talk_charge
      , 1 as is_recent
FROM (
 SELECT bundle_code
      , case
            when bundle_code > 1 then 'bundle'
            when bundle_code <= 1 then 'payg'
            else 'unknown' end as user_type
      , network_id
      , voice_call_cdr --cdr_type
      , call_type
      , call_feature
      , tariffplan_id
      , roam_flag
      , roaming_area_code
      , roaming_zone_name
      , roaming_partner_name
      , destinationzone_name
      , destination_area_code
      , destination_zone
      , call_date_hour    -----sortkey 4
      , call_date_dt
      , call_date_num           -------dist key --------sortkey 3
      , call_date_month                             ------sort key 2
      , CAST(LEFT(call_date_month, 4) as INT) as call_date_year
      , ddm.country_name
      , CAST(sum(call_duration_min) AS DECIMAL(22, 6)) as total_call_duration_min
      , CAST(sum(chargeable_used_time_min) AS DECIMAL(22, 6)) as total_chargeable_used_time_min
      , CAST(sum(talk_charge) AS DECIMAL(22, 6)) as total_talk_charge
 FROM {fact_table} frv
          left outer join {dim_destination_master} ddm
                    on left(trim(destinationzone_name), 3) = trim(ddm.country_code)
                    and ddm.is_recent = 1
 group by bundle_code
        , user_type
        , network_id
        , voice_call_cdr --cdr_type
        , call_type
        , call_feature
        , tariffplan_id
        , roam_flag
        , roaming_area_code
        , roaming_zone_name
        , roaming_partner_name
        , destinationzone_name
        , destination_area_code
        , destination_zone
        , call_date_hour
        , call_date_dt
        , call_date_num
        , call_date_month
        , call_date_year
        , ddm.country_name
    )
"""

rrbs_voice_daily = """
SELECT 
      bundle_code
      , user_type
      , network_id
      , voice_call_cdr --cdr_type
      , call_type
      , call_feature
      , tariffplan_id
      , roam_flag
      , roaming_area_code
      , roaming_zone_name
      , roaming_partner_name
      , destinationzone_name as destination_zone_name
      , destination_area_code
      , destination_zone
      , call_date_dt
      , call_date_num  
      , call_date_month 
      , call_date_year
      , country_name
      , total_call_duration_min
      , total_chargeable_used_time_min
      , total_talk_charge
      , 1 as is_recent
FROM (
 SELECT bundle_code
      , case
            when bundle_code > 1 then 'bundle'
            when bundle_code <= 1 then 'payg'
            else 'unknown' end as user_type
      , network_id
      , voice_call_cdr --cdr_type
      , call_type
      , call_feature
      , tariffplan_id
      , roam_flag
      , roaming_area_code
      , roaming_zone_name
      , roaming_partner_name
      , destinationzone_name
      , destination_area_code
      , destination_zone
      , call_date_dt
      , call_date_num           -------dist key --------sortkey 3
      , call_date_month                             ------sort key 2
      , CAST(LEFT(call_date_month, 4) as INT) as call_date_year
      , ddm.country_name
      , CAST(sum(call_duration_min) AS DECIMAL(22, 6)) as total_call_duration_min
      , CAST(sum(chargeable_used_time_min) AS DECIMAL(22, 6)) as total_chargeable_used_time_min
      , CAST(sum(talk_charge) AS DECIMAL(22, 6)) as total_talk_charge
      , 1 as is_recent -- sortkey 1
 FROM {fact_table} frv
          left outer join {dim_destination_master} ddm
                    on left(trim(destinationzone_name), 3) = trim(ddm.country_code)
                    and ddm.is_recent = 1
 group by bundle_code
        , user_type
        , network_id
        , voice_call_cdr --cdr_type
        , call_type
        , call_feature
        , tariffplan_id
        , roam_flag
        , roaming_area_code
        , roaming_zone_name
        , roaming_partner_name
        , destinationzone_name
        , destination_area_code
        , destination_zone
        , call_date_dt
        , call_date_num
        , call_date_month
        , call_date_year
        , ddm.country_name
    )
"""

rrbs_voice_monthly = """
SELECT 
      bundle_code
      , user_type
      , network_id
      , voice_call_cdr --cdr_type
      , call_type
      , call_feature
      , tariffplan_id
      , roam_flag
      , roaming_area_code
      , roaming_zone_name
      , roaming_partner_name
      , destinationzone_name as destination_zone_name
      , destination_area_code
      , destination_zone
      , call_date_month 
      , call_date_year
      , country_name
      , total_call_duration_min
      , total_chargeable_used_time_min
      , total_talk_charge
      , 1 as is_recent
FROM (
 SELECT bundle_code
      , case
            when bundle_code > 1 then 'bundle'
            when bundle_code <= 1 then 'payg'
            else 'unknown' end as user_type
      , network_id
      , voice_call_cdr --cdr_type
      , call_type
      , call_feature
      , tariffplan_id
      , roam_flag
      , roaming_area_code
      , roaming_zone_name
      , roaming_partner_name
      , destinationzone_name
      , destination_area_code
      , destination_zone
      , call_date_month
      , CAST(LEFT(call_date_month, 4) as INT) as call_date_year
      , ddm.country_name
      , CAST(sum(call_duration_min) AS DECIMAL(22, 6)) as total_call_duration_min
      , CAST(sum(chargeable_used_time_min) AS DECIMAL(22, 6)) as total_chargeable_used_time_min
      , CAST(sum(talk_charge) AS DECIMAL(22, 6)) as total_talk_charge
      , 1 as is_recent -- sortkey 1
 FROM {fact_table} frv
          left outer join {dim_destination_master} ddm
                    on left(trim(destinationzone_name), 3) = trim(ddm.country_code)
                    and ddm.is_recent = 1
 group by bundle_code
        , user_type
        , network_id
        , voice_call_cdr --cdr_type
        , call_type
        , call_feature
        , tariffplan_id
        , roam_flag
        , roaming_area_code
        , roaming_zone_name
        , roaming_partner_name
        , destinationzone_name
        , destination_area_code
        , destination_zone
        , call_date_month
        , call_date_year
        , ddm.country_name
    )
"""

rrbs_voice_yearly = """
SELECT 
      bundle_code
      , user_type
      , network_id
      , voice_call_cdr --cdr_type
      , call_type
      , call_feature
      , tariffplan_id
      , roam_flag
      , roaming_area_code
      , roaming_zone_name
      , roaming_partner_name
      , destinationzone_name as destination_zone_name
      , destination_area_code
      , destination_zone
      , call_date_year
      , country_name
      , total_call_duration_min
      , total_chargeable_used_time_min
      , total_talk_charge
      , 1 as is_recent
FROM (
 SELECT bundle_code
      , case
            when bundle_code > 1 then 'bundle'
            when bundle_code <= 1 then 'payg'
            else 'unknown' end as user_type
      , network_id
      , voice_call_cdr --cdr_type
      , call_type
      , call_feature
      , tariffplan_id
      , roam_flag
      , roaming_area_code
      , roaming_zone_name
      , roaming_partner_name
      , destinationzone_name
      , destination_area_code
      , destination_zone
      , CAST(LEFT(call_date_month, 4) as INT) as call_date_year                          
      , ddm.country_name
      , CAST(sum(call_duration_min) AS DECIMAL(22, 6)) as total_call_duration_min
      , CAST(sum(chargeable_used_time_min) AS DECIMAL(22, 6)) as total_chargeable_used_time_min
      , CAST(sum(talk_charge) AS DECIMAL(22, 6)) as total_talk_charge
      , 1 as is_recent -- sortkey 1
 FROM {fact_table} frv
          left outer join {dim_destination_master} ddm
                    on left(trim(destinationzone_name), 3) = trim(ddm.country_code)
                    and ddm.is_recent = 1
 group by bundle_code
        , user_type
        , network_id
        , voice_call_cdr --cdr_type
        , call_type
        , call_feature
        , tariffplan_id
        , roam_flag
        , roaming_area_code
        , roaming_zone_name
        , roaming_partner_name
        , destinationzone_name
        , destination_area_code
        , destination_zone
        , call_date_year
        , ddm.country_name
    )
"""