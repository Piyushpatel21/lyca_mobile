CREATE TABLE temp_factagg_user_rrbs_gprs_term_yearly (
    msisdn                      bigint encode az64
    , user_type                 varchar(30) encode zstd
    , bundle_code               varchar(20)
    , tariffplan_id             smallint encode az64
    , cdr_type                  smallint encode az64
    , network_id                smallint encode az64
    , data_termination_year     integer encode az64
)
diststyle even
;