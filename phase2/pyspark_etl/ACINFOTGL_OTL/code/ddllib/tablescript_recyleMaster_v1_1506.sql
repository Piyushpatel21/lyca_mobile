create table if not exists uk_rrbs_dm.tgl_recycle_master
(
    ID	INT	encode az64
    ,MONTH	varchar(50)	encode zstd
    ,NETWORK_ID	smallint	encode az64
    ,MSISDN	bigint	encode az64
    ,MSISDN_SWAP	varchar(50)	encode zstd
    ,ICCID_PREFIX	bigint	encode az64
    ,ICCID	bigint	encode az64
    ,IMSI	bigint	encode az64
    ,IMSI2	bigint	encode az64
    ,LAST_USAGE_DATE	timestamp	encode az64
    ,BALANCE	numeric(22,6)	encode az64
    ,PRODUCTION_STATUS	varchar(100)	encode zstd
    ,ACTIVATION_DATE	timestamp	encode az64
    ,LAST_CALL_DATE	VARCHAR(50)	encode zstd
    ,LAST_MSG_DATE	VARCHAR(50)	encode zstd
    ,LAST_TOP_DATE	VARCHAR(50)	encode zstd
    ,HLRID	VARCHAR(50)	encode zstd
    ,Deactivation_date	VARCHAR(50)	encode zstd
    ,INITIALBAL	VARCHAR(50)	encode zstd
    ,ACC_ID	VARCHAR(50)	encode zstd
    ,ResellerId	VARCHAR(50)	encode zstd
    ,Offmgrid	VARCHAR(50)	encode zstd
    ,Accmgrid	VARCHAR(50)	encode zstd
    ,Hotspotid	VARCHAR(50)	encode zstd
    ,Retailerid	VARCHAR(50)	encode zstd
    ,DISTRIBUTORID	VARCHAR(50)	encode zstd
    ,VALID_MONTH	INT	encode az64
    ,VALID_DATE	DATE	encode az64
    ,batch_id	INTEGER	encode az64
    ,created_DATE	TIMESTAMP	encode az64
    ,rec_checksum	VARCHAR(32)	encode zstd
);

create table if not exists uk_rrbs_dm.tgl_recycle_master_duplcdr
(
    ID	INT	encode az64
    ,MONTH	varchar(50)	encode zstd
    ,NETWORK_ID	smallint	encode az64
    ,MSISDN	bigint	encode az64
    ,MSISDN_SWAP	varchar(50)	encode zstd
    ,ICCID_PREFIX	bigint	encode az64
    ,ICCID	bigint	encode az64
    ,IMSI	bigint	encode az64
    ,IMSI2	bigint	encode az64
    ,LAST_USAGE_DATE	timestamp	encode az64
    ,BALANCE	numeric(22,6)	encode az64
    ,PRODUCTION_STATUS	varchar(100)	encode zstd
    ,ACTIVATION_DATE	timestamp	encode az64
    ,LAST_CALL_DATE	VARCHAR(50)	encode zstd
    ,LAST_MSG_DATE	VARCHAR(50)	encode zstd
    ,LAST_TOP_DATE	VARCHAR(50)	encode zstd
    ,HLRID	VARCHAR(50)	encode zstd
    ,Deactivation_date	VARCHAR(50)	encode zstd
    ,INITIALBAL	VARCHAR(50)	encode zstd
    ,ACC_ID	VARCHAR(50)	encode zstd
    ,ResellerId	VARCHAR(50)	encode zstd
    ,Offmgrid	VARCHAR(50)	encode zstd
    ,Accmgrid	VARCHAR(50)	encode zstd
    ,Hotspotid	VARCHAR(50)	encode zstd
    ,Retailerid	VARCHAR(50)	encode zstd
    ,DISTRIBUTORID	VARCHAR(50)	encode zstd
    ,VALID_MONTH	INT	encode az64
    ,VALID_DATE	DATE	encode az64
    ,batch_id	INTEGER	encode az64
    ,created_DATE	TIMESTAMP	encode az64
    ,rec_checksum	VARCHAR(32)	encode zstd
);