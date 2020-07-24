CREATE TABLE temp_factagg_user_rrbs_voice_monthly (
    charged_party_number        bigint encode az64
    , user_type                 varchar(30) encode zstd
    , bundle_code               varchar(20) encode zstd
    , tariffplan_id             integer encode az64
    , call_type                 integer
    , voice_call_cdr            integer encode az64
    , destination_zone          integer encode az64
    , destination_area_code     bigint encode az64
    , destination_zone_name      varchar(30) encode bytedict
    , roam_flag                 smallint
    , network_id                integer
    , call_feature              smallint
    , call_date_month           integer encode az64
    , call_date_year            integer encode az64
)
diststyle even
;