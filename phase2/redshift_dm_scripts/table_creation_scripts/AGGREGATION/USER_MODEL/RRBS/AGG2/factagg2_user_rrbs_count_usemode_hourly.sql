CREATE TABLE factagg2_user_rrbs_count_usemode_hourly (
    sk_id                   bigint generated by default as identity (1,1) encode az64
    , use_mode              varchar(30) encode zstd
    , user_count            integer encode az64
    , call_date_hour        integer encode az64
    , call_date_num         integer encode az64
    , call_date_month       integer encode az64
    , call_date_year        integer encode az64
    , is_recent             smallint
    , created_date_time     timestamp  default sysdate encode
    , created_date          date default sysdate
)
diststyle even
sortkey
(is_recent, call_date_num, call_date_hour)
;