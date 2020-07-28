CREATE TABLE factagg_usage_rrbs_voice_hourly (
    sk_id                               bigint generated by default as identity (1,1) encode az64
    , bundle_code                       varchar(20) encode zstd
    , user_type                         varchar(30) encode zstd
    , network_id                        integer
    , voice_call_cdr                    integer encode az64
    , call_type                         integer
    , call_feature                      smallint
    , tariffplan_id                     integer encode az64
    , roam_flag                         smallint
    , roaming_area_code                 varchar(20) encode zstd
    , roaming_zone_name                 varchar(30) encode zstd
    , roaming_partner_name              varchar(50) encode zstd
    , destination_zone_name             varchar(30) encode bytedict
    , destination_area_code             bigint encode az64
    , destination_zone                  integer encode az64
    , call_date_hour                    integer encode az64
    , call_date_dt                      date encode az64
    , call_date_num                     integer encode az64
    , call_date_month                   integer encode az64
    , call_date_year                    integer encode az64
    , country_name                      varchar(50) encode zstd
    , total_call_duration_min           decimal(22,6) encode zstd
    , total_chargeable_used_time_min    decimal(22,6) encode zstd
    , total_talk_charge                 decimal(22,6) encode zstd
    , is_recent                         smallint
    , created_date_time                 timestamp  default sysdate encode
    , created_date                      date default sysdate
)
diststyle even
sortkey
(is_recent, call_date_month, call_date_num, call_date_hour)
;