CREATE TABLE uk_rrbs_dm.tgl_srt_trnvouchers_used_log
(
    voucherid BIGINT encode az64
    ,card_id VARCHAR(4) encode zstd
    ,source_batch_id VARCHAR(8) encode zstd
    ,Serial_id VARCHAR(4) encode zstd
    ,CardNumber VARCHAR(50) encode zstd
    ,Card_Pin VARCHAR(50) encode zstd
    ,Face_Value DECIMAL(38,10) encode az64
    ,Card_Currency VARCHAR(50) encode zstd
    ,Create_Date DATETIME encode az64
    ,Expired_Date DATETIME encode az64
    ,Reseller_id VARCHAR(50) encode zstd
    ,SiteCode VARCHAR(3) encode zstd
    ,Printed_Price DECIMAL(38,10) encode az64
    ,Validity VARCHAR(4) encode zstd
    ,VoucherStatusID INT encode az64
    ,UsedDate DATETIME encode az64
    ,MSISDN VARCHAR(20) encode zstd
    ,Plan_ID INT encode az64
    ,Is_SBTPrintable SMALLINT encode az64
    ,Is_Bundle INT encode az64
    ,Is_Assigned SMALLINT encode az64
    ,BUNDLE_CODE INT encode az64
    ,transaction_ID VARCHAR(50) encode zstd
    ,serviceID INT encode az64
    ,VoucherType INT encode az64
    ,BundleCost DECIMAL(38,10) encode az64
    ,RecycledBy VARCHAR(200) encode zstd
    ,RecycledOn DATETIME encode az64
    ,UsedDate_month INT encode az64
    ,batch_id INTEGER
    ,created_DATE TIMESTAMP
    ,rec_checksum VARCHAR(32)
);

CREATE TABLE uk_rrbs_dm.tgl_srt_trnvouchers_used_log_duplcdr
(
    voucherid BIGINT encode az64
    ,card_id VARCHAR(4) encode zstd
    ,source_batch_id VARCHAR(8) encode zstd
    ,Serial_id VARCHAR(4) encode zstd
    ,CardNumber VARCHAR(50) encode zstd
    ,Card_Pin VARCHAR(50) encode zstd
    ,Face_Value DECIMAL(38,10) encode az64
    ,Card_Currency VARCHAR(50) encode zstd
    ,Create_Date DATETIME encode az64
    ,Expired_Date DATETIME encode az64
    ,Reseller_id VARCHAR(50) encode zstd
    ,SiteCode VARCHAR(3) encode zstd
    ,Printed_Price DECIMAL(38,10) encode az64
    ,Validity VARCHAR(4) encode zstd
    ,VoucherStatusID INT encode az64
    ,UsedDate DATETIME encode az64
    ,MSISDN VARCHAR(20) encode zstd
    ,Plan_ID INT encode az64
    ,Is_SBTPrintable SMALLINT encode az64
    ,Is_Bundle INT encode az64
    ,Is_Assigned SMALLINT encode az64
    ,BUNDLE_CODE INT encode az64
    ,transaction_ID VARCHAR(50) encode zstd
    ,serviceID INT encode az64
    ,VoucherType INT encode az64
    ,BundleCost DECIMAL(38,10) encode az64
    ,RecycledBy VARCHAR(200) encode zstd
    ,RecycledOn DATETIME encode az64
    ,UsedDate_month INT encode az64
    ,batch_id INTEGER
    ,created_DATE TIMESTAMP
    ,rec_checksum VARCHAR(32)
);