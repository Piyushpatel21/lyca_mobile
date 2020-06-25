CREATE TABLE uk_rrbs_dm.tgl_recycled_firstusage
(
    ID INT encode az64
    ,MONTH VARCHAR(20) encode zstd
    ,NETWORK_ID VARCHAR(10) encode zstd
    ,MSISDN VARCHAR(20) encode zstd
    ,MSISDN_SWAP VARCHAR(20) encode zstd
    ,ICCIDPREFIX VARCHAR(20) encode zstd
    ,ICCID VARCHAR(30) encode zstd
    ,IMSI VARCHAR(30) encode zstd
    ,IMSI2 VARCHAR(30) encode zstd
    ,LAST_USAGE_DATE DATETIME encode az64
    ,BALANCE DECIMAL encode az64
    ,PRODUCTION_STATUS VARCHAR(100) encode zstd
    ,ACTIVATIONDATE DATETIME encode az64
    ,REGISTRATIONDATE DATETIME encode az64
    ,LAST_CALL_DATE VARCHAR(30) encode zstd
    ,LAST_MSG_DATE VARCHAR(30) encode zstd
    ,LAST_TOP_DATE VARCHAR(30) encode zstd
    ,HLRID VARCHAR(10) encode zstd
    ,DeactivationDate DATETIME encode az64
    ,INITIALBAL DECIMAL encode az64
    ,ACC_ID VARCHAR(50) encode zstd
    ,ResellerId VARCHAR(50) encode zstd
    ,Offmgrid VARCHAR(50) encode zstd
    ,Accmgrid VARCHAR(50) encode zstd
    ,Hotspotid VARCHAR(50) encode zstd
    ,Retailerid VARCHAR(50) encode zstd
    ,DISTRIBUTORID VARCHAR(50) encode zstd
    ,FILEDATE DATE encode az64
    ,FACEVALUE DECIMAL encode az64
    ,FIRST_ICCID VARCHAR(100) encode zstd
    ,batch_id INTEGER
    ,created_DATE TIMESTAMP
    ,rec_checksum VARCHAR(32)
);

CREATE TABLE uk_rrbs_dm.tgl_recycled_firstusage_duplcdr
(
    ID INT encode az64
    ,MONTH VARCHAR(20) encode zstd
    ,NETWORK_ID VARCHAR(10) encode zstd
    ,MSISDN VARCHAR(20) encode zstd
    ,MSISDN_SWAP VARCHAR(20) encode zstd
    ,ICCIDPREFIX VARCHAR(20) encode zstd
    ,ICCID VARCHAR(30) encode zstd
    ,IMSI VARCHAR(30) encode zstd
    ,IMSI2 VARCHAR(30) encode zstd
    ,LAST_USAGE_DATE DATETIME encode az64
    ,BALANCE DECIMAL encode az64
    ,PRODUCTION_STATUS VARCHAR(100) encode zstd
    ,ACTIVATIONDATE DATETIME encode az64
    ,REGISTRATIONDATE DATETIME encode az64
    ,LAST_CALL_DATE VARCHAR(30) encode zstd
    ,LAST_MSG_DATE VARCHAR(30) encode zstd
    ,LAST_TOP_DATE VARCHAR(30) encode zstd
    ,HLRID VARCHAR(10) encode zstd
    ,DeactivationDate DATETIME encode az64
    ,INITIALBAL DECIMAL encode az64
    ,ACC_ID VARCHAR(50) encode zstd
    ,ResellerId VARCHAR(50) encode zstd
    ,Offmgrid VARCHAR(50) encode zstd
    ,Accmgrid VARCHAR(50) encode zstd
    ,Hotspotid VARCHAR(50) encode zstd
    ,Retailerid VARCHAR(50) encode zstd
    ,DISTRIBUTORID VARCHAR(50) encode zstd
    ,FILEDATE DATE encode az64
    ,FACEVALUE DECIMAL encode az64
    ,FIRST_ICCID VARCHAR(100) encode zstd
    ,batch_id INTEGER
    ,created_DATE TIMESTAMP
    ,rec_checksum VARCHAR(32)
);