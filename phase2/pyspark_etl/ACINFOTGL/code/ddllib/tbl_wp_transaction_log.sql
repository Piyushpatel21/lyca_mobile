CREATE TABLE uk_rrbs_dm.tgl_tbl_wp_transaction_log
(
    Id INT encode az64
    ,SubscriberId VARCHAR(20) encode zstd
    ,CardNumber VARCHAR(200) encode zstd
    ,TransactionId VARCHAR(30) encode zstd
    ,TransactionType VARCHAR(2) encode zstd
    ,OrderId VARCHAR(20) encode zstd
    ,OrderDescription VARCHAR(50) encode zstd
    ,OrderDateTime DATETIME encode az64
    ,MerchantCode VARCHAR(30) encode zstd
    ,PaymentMethodCode VARCHAR(30) encode zstd
    ,StatusCode VARCHAR(30) encode zstd
    ,ResponseCode VARCHAR(10) encode zstd
    ,ResponseDescription VARCHAR(1000) encode zstd
    ,ResponseDateTime DATETIME encode az64
    ,SessionId VARCHAR(50) encode zstd
    ,EchoData VARCHAR(20) encode zstd
    ,RiskScore VARCHAR(10) encode zstd
    ,CVCResultCode VARCHAR(50) encode zstd
    ,AVSResultCode VARCHAR(50) encode zstd
    ,AAVAddressResultCode VARCHAR(50) encode zstd
    ,AAVPostcodeResultCode VARCHAR(50) encode zstd
    ,AAVCardholderNameResultCode VARCHAR(50) encode zstd
    ,AAVTelephoneResultCode VARCHAR(50) encode zstd
    ,AAVEmailResultCode VARCHAR(50) encode zstd
    ,CreatedDate DATETIME encode az64
    ,APMmac VARCHAR(50) encode zstd
    ,APMReferenceId VARCHAR(50) encode zstd
    ,IssuerURL VARCHAR(1000) encode zstd
    ,RGProfileID VARCHAR(50) encode zstd
    ,vatid VARCHAR(30) encode zstd
    ,CapturedRequest VARCHAR(5) encode zstd
    ,CapturedResponse VARCHAR(50) encode zstd
    ,batch_id INTEGER
    ,created_DATE TIMESTAMP
    ,rec_checksum VARCHAR(32)
 );

CREATE TABLE uk_rrbs_dm.tgl_tbl_wp_transaction_log_duplcdr
(
    Id INT encode az64
    ,SubscriberId VARCHAR(20) encode zstd
    ,CardNumber VARCHAR(200) encode zstd
    ,TransactionId VARCHAR(30) encode zstd
    ,TransactionType VARCHAR(2) encode zstd
    ,OrderId VARCHAR(20) encode zstd
    ,OrderDescription VARCHAR(50) encode zstd
    ,OrderDateTime DATETIME encode az64
    ,MerchantCode VARCHAR(30) encode zstd
    ,PaymentMethodCode VARCHAR(30) encode zstd
    ,StatusCode VARCHAR(30) encode zstd
    ,ResponseCode VARCHAR(10) encode zstd
    ,ResponseDescription VARCHAR(1000) encode zstd
    ,ResponseDateTime DATETIME encode az64
    ,SessionId VARCHAR(50) encode zstd
    ,EchoData VARCHAR(20) encode zstd
    ,RiskScore VARCHAR(10) encode zstd
    ,CVCResultCode VARCHAR(50) encode zstd
    ,AVSResultCode VARCHAR(50) encode zstd
    ,AAVAddressResultCode VARCHAR(50) encode zstd
    ,AAVPostcodeResultCode VARCHAR(50) encode zstd
    ,AAVCardholderNameResultCode VARCHAR(50) encode zstd
    ,AAVTelephoneResultCode VARCHAR(50) encode zstd
    ,AAVEmailResultCode VARCHAR(50) encode zstd
    ,CreatedDate DATETIME encode az64
    ,APMmac VARCHAR(50) encode zstd
    ,APMReferenceId VARCHAR(50) encode zstd
    ,IssuerURL VARCHAR(1000) encode zstd
    ,RGProfileID VARCHAR(50) encode zstd
    ,vatid VARCHAR(30) encode zstd
    ,CapturedRequest VARCHAR(5) encode zstd
    ,CapturedResponse VARCHAR(50) encode zstd
    ,batch_id INTEGER
    ,created_DATE TIMESTAMP
    ,rec_checksum VARCHAR(32)
 );
