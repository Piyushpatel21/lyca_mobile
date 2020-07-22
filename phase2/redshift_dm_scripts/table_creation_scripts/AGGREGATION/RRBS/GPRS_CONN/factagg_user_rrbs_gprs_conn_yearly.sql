CREATE TABLE factagg_user_rrbs_gprs_conn_yearly (
    sk_id                       bigint generated by default as identity (1,1) encode az64
    , msisdn                    bigint encode az64
    , user_type                 varchar(30) encode zstd
    , bundle_code               varchar(20) encode zstd
    , tariffplan_id             smallint encode az64
    , cdr_type                  smallint encode az64
    , network_id                smallint encode az64
    , data_connection_year      integer encode az64
    , created_date_time         timestamp  default sysdate encode
    , created_date              date default sysdate
)
diststyle even
sortkey
(data_connection_year)
;