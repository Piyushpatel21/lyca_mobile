CREATE TABLE factagg2_user_rrbs_count_call_user_usage_monthly (
    sk_id                   bigint generated by default as identity (1,1) encode az64
    , ct					integer encode az64
    , user_type             varchar(30) encode zstd
    , use_mode				varchar(30) encode zstd
    , call_type				varchar(50) encode zstd
    , call_date_month       integer encode az64
    , call_date_year        integer encode az64
    , is_recent             smallint
    , created_date_time     timestamp  default sysdate encode
    , created_date          date default sysdate
)
diststyle even
sortkey
(is_recent, call_date_year, call_date_month)
;