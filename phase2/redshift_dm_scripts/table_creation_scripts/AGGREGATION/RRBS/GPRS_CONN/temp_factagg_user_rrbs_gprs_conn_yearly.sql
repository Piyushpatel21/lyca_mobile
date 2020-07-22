CREATE TABLE temp_factagg_user_rrbs_gprs_conn_yearly (
    msisdn                    bigint encode az64
    , user_type                 varchar(30) encode zstd
    , bundle_code               varchar(20) encode zstd
    , tariffplan_id             smallint encode az64
    , cdr_type                  smallint encode az64
    , network_id                smallint encode az64
    , data_connection_year      integer encode az64
)
diststyle even
;