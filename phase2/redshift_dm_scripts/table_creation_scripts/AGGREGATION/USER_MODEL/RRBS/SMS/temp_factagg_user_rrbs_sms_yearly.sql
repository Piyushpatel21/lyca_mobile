CREATE TABLE temp_factagg_user_rrbs_sms_yearly (
    cli                       bigint encode az64
    , user_type                 varchar(30) encode zstd
    , bundle_code               varchar(20) encode zstd
    , plan_id                   integer encode az64
    , call_type                 integer
    , cdr_types                 integer encode az64
    , destination_zone_code     integer encode az64
    , destination_area_code     integer encode az64
    , destination_zone_name     varchar(50) encode bytedict
    , network_id                smallint
    , roam_flag                 smallint
    , sms_feature               smallint
    , msg_date_year             integer encode az64
)
diststyle even
;