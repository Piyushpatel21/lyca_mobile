CREATE TABLE temp_factagg_user_rrbs_sms_daily (
    cli                       bigint encode az64
    , user_type                 varchar(30) encode zstd
    , bundle_code               integer encode az64
    , call_type                 integer
    , cdr_types                 integer encode az64
    , destination_zone_code     integer encode az64
    , destination_area_code     integer encode az64
    , destination_zone_name     varchar(50) encode bytedict
    , network_id                smallint
    , msg_date_dt               date encode az64
    , msg_date_num              integer encode az64
    , msg_date_month            integer encode az64
    , msg_date_year             integer encode az64
)
diststyle even
;