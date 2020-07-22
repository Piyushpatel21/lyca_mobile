CREATE TABLE temp_factagg_user_rrbs_gprs_term_hourly (
    msisdn                      bigint encode az64
    , user_type                 varchar(30) encode zstd
    , bundle_code               varchar(20)
    , tariffplan_id             smallint encode az64
    , cdr_type                  smallint encode az64
    , network_id                smallint encode az64
    , data_termination_hour     integer encode az64
    , data_termination_dt       date encode az64
    , data_termination_dt_num   integer encode az64
    , data_termination_month    integer encode az64
    , data_termination_year     integer encode az64
)
diststyle even
;