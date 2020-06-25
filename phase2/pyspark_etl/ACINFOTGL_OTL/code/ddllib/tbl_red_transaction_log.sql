CREATE TABLE uk_rrbs_dm.tgl_tbl_red_transaction_log
(
    Id INT encode az64
	,SubscriberId VARCHAR(20) encode zstd
	,CardNumber VARCHAR(200) encode zstd
	,TransactionId VARCHAR(30) encode zstd
	,TransactionType VARCHAR(2) encode zstd
	,RedOrderId VARCHAR(20) encode zstd
	,DivisionNumber VARCHAR(20) encode zstd
	,ReDKeyId INT encode az64
	,RequestId VARCHAR(20) encode zstd
	,RequestTypeCode VARCHAR(20) encode zstd
	,StatusCode VARCHAR(20) encode zstd
	,ResponseCode VARCHAR(20) encode zstd
	,ResponseDate DATETIME encode az64
	,FraudStatusCode VARCHAR(20) encode zstd
	,RedTDSStatusCode VARCHAR(20) encode zstd
	,RedDTSSIG VARCHAR(20) encode zstd
	,RedTDSECI VARCHAR(20) encode zstd
	,RedOrderDateTime DATETIME encode az64
	,RedAUTHNumber VARCHAR(20) encode zstd
	,RedResponseMessage VARCHAR(50) encode zstd
	,RedAVSCode VARCHAR(20) encode zstd
	,RedSecCode VARCHAR(20) encode zstd
	,FraudResponseCode VARCHAR(20) encode zstd
	,FraudUseCode VARCHAR(20) encode zstd
	,FraudRCF VARCHAR(20) encode zstd
	,CreatedDate DATETIME encode az64
	,TDSACSURL VARCHAR(max) encode zstd
	,EBT_Name VARCHAR(50) encode zstd
	,EBT_Service VARCHAR(50) encode zstd
	,vatid VARCHAR(30) encode zstd
	,ConnectorTxnId VARCHAR(50) encode zstd
	,bankName VARCHAR(100) encode zstd
	,BankCountry VARCHAR(50) encode zstd
	,buildNumber VARCHAR(20) encode zstd
	,ndc VARCHAR(50) encode zstd
	,RiskScore VARCHAR(10) encode zstd
	,batch_id INTEGER
	,created_DATE TIMESTAMP
	,rec_checksum VARCHAR(32)
);

CREATE TABLE uk_rrbs_dm.tgl_tbl_red_transaction_log_duplcdr
(
    Id INT encode az64
	,SubscriberId VARCHAR(20) encode zstd
	,CardNumber VARCHAR(200) encode zstd
	,TransactionId VARCHAR(30) encode zstd
	,TransactionType VARCHAR(2) encode zstd
	,RedOrderId VARCHAR(20) encode zstd
	,DivisionNumber VARCHAR(20) encode zstd
	,ReDKeyId INT encode az64
	,RequestId VARCHAR(20) encode zstd
	,RequestTypeCode VARCHAR(20) encode zstd
	,StatusCode VARCHAR(20) encode zstd
	,ResponseCode VARCHAR(20) encode zstd
	,ResponseDate DATETIME encode az64
	,FraudStatusCode VARCHAR(20) encode zstd
	,RedTDSStatusCode VARCHAR(20) encode zstd
	,RedDTSSIG VARCHAR(20) encode zstd
	,RedTDSECI VARCHAR(20) encode zstd
	,RedOrderDateTime DATETIME encode az64
	,RedAUTHNumber VARCHAR(20) encode zstd
	,RedResponseMessage VARCHAR(50) encode zstd
	,RedAVSCode VARCHAR(20) encode zstd
	,RedSecCode VARCHAR(20) encode zstd
	,FraudResponseCode VARCHAR(20) encode zstd
	,FraudUseCode VARCHAR(20) encode zstd
	,FraudRCF VARCHAR(20) encode zstd
	,CreatedDate DATETIME encode az64
	,TDSACSURL VARCHAR(max) encode zstd
	,EBT_Name VARCHAR(50) encode zstd
	,EBT_Service VARCHAR(50) encode zstd
	,vatid VARCHAR(30) encode zstd
	,ConnectorTxnId VARCHAR(50) encode zstd
	,bankName VARCHAR(100) encode zstd
	,BankCountry VARCHAR(50) encode zstd
	,buildNumber VARCHAR(20) encode zstd
	,ndc VARCHAR(50) encode zstd
	,RiskScore VARCHAR(10) encode zstd
	,batch_id INTEGER
	,created_DATE TIMESTAMP
	,rec_checksum VARCHAR(32)
);