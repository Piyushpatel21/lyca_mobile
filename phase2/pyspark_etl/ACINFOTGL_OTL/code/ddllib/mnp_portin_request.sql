CREATE TABLE uk_rrbs_dm.tgl_mnp_portin_request
(
    id BIGINT encode az64
    ,RequestId VARCHAR(30) encode zstd
    ,MSISDN VARCHAR(20) encode zstd
    ,IMSI VARCHAR(50) encode zstd
    ,ICCID VARCHAR(20) encode zstd
    ,PICCID VARCHAR(20) encode zstd
    ,PMSISDN VARCHAR(20) encode zstd
    ,Channel VARCHAR(10) encode zstd
    ,RequestDate DATETIME encode az64
    ,STATUS INT encode az64
    ,SMSstate INT encode az64
    ,ErrCode VARCHAR(50) encode zstd
    ,RecordLock INT encode az64
    ,MsgTime DATETIME encode az64
    ,RNO VARCHAR(4) encode zstd
    ,DNO VARCHAR(4) encode zstd
    ,ONO VARCHAR(4) encode zstd
    ,RSP VARCHAR(4) encode zstd
    ,DSP VARCHAR(4) encode zstd
    ,Portindate DATETIME encode az64
    ,Note VARCHAR(100) encode zstd
    ,Balance VARCHAR(50) encode zstd
    ,OldBalance VARCHAR(50) encode zstd
    ,NewBalance VARCHAR(50) encode zstd
    ,PAC VARCHAR(50) encode zstd
    ,PACExpiry DATETIME encode az64
    ,CreatedDate DATETIME encode az64
    ,CreatedBy VARCHAR(100) encode zstd
    ,CompletedDate DATETIME encode az64
    ,RequestFrom VARCHAR(20) encode zstd
    ,FirstName VARCHAR(100) encode zstd
    ,LastName VARCHAR(100) encode zstd
    ,Email VARCHAR(150) encode zstd
    ,RejectCode VARCHAR(10) encode zstd
    ,AttachedDocumentName VARCHAR(100) encode zstd
    ,PreferredLanguage VARCHAR(20) encode zstd
    ,SmsErrState VARCHAR(10) encode zstd
    ,CIP VARCHAR(10)
    ,OriginalMSISDN VARCHAR(50) encode zstd
    ,NType VARCHAR(10)
    ,completeddate_month INT --(yyyymm)
    ,batch_id INTEGER
    ,created_DATE TIMESTAMP
    ,rec_checksum VARCHAR(32)
);

CREATE TABLE uk_rrbs_dm.tgl_mnp_portin_request_duplcdr
(
    id BIGINT encode az64
    ,RequestId VARCHAR(30) encode zstd
    ,MSISDN VARCHAR(20) encode zstd
    ,IMSI VARCHAR(50) encode zstd
    ,ICCID VARCHAR(20) encode zstd
    ,PICCID VARCHAR(20) encode zstd
    ,PMSISDN VARCHAR(20) encode zstd
    ,Channel VARCHAR(10) encode zstd
    ,RequestDate DATETIME encode az64
    ,STATUS INT encode az64
    ,SMSstate INT encode az64
    ,ErrCode VARCHAR(50) encode zstd
    ,RecordLock INT encode az64
    ,MsgTime DATETIME encode az64
    ,RNO VARCHAR(4) encode zstd
    ,DNO VARCHAR(4) encode zstd
    ,ONO VARCHAR(4) encode zstd
    ,RSP VARCHAR(4) encode zstd
    ,DSP VARCHAR(4) encode zstd
    ,Portindate DATETIME encode az64
    ,Note VARCHAR(100) encode zstd
    ,Balance VARCHAR(50) encode zstd
    ,OldBalance VARCHAR(50) encode zstd
    ,NewBalance VARCHAR(50) encode zstd
    ,PAC VARCHAR(50) encode zstd
    ,PACExpiry DATETIME encode az64
    ,CreatedDate DATETIME encode az64
    ,CreatedBy VARCHAR(100) encode zstd
    ,CompletedDate DATETIME encode az64
    ,RequestFrom VARCHAR(20) encode zstd
    ,FirstName VARCHAR(100) encode zstd
    ,LastName VARCHAR(100) encode zstd
    ,Email VARCHAR(150) encode zstd
    ,RejectCode VARCHAR(10) encode zstd
    ,AttachedDocumentName VARCHAR(100) encode zstd
    ,PreferredLanguage VARCHAR(20) encode zstd
    ,SmsErrState VARCHAR(10) encode zstd
    ,CIP VARCHAR(10)
    ,OriginalMSISDN VARCHAR(50) encode zstd
    ,NType VARCHAR(10)
    ,completeddate_month INT --(yyyymm)
    ,batch_id INTEGER
    ,created_DATE TIMESTAMP
    ,rec_checksum VARCHAR(32)
);