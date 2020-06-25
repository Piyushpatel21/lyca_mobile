CREATE TABLE uk_rrbs_dm.tgl_tble_shop_transaction_status
(
    Id INT encode az64
	,TransactionId VARCHAR(30) encode zstd
	,SubscriberId VARCHAR(20) encode zstd
	,CardNumber VARCHAR(200) encode zstd
	,CustomerIPAddress VARCHAR(20) encode zstd
	,OrderId VARCHAR(20) encode zstd
	,CountryId INT encode az64
	,BrandId INT encode az64
	,ChannelId INT encode az64
	,GatewayId INT encode az64
	,FeatureId INT encode az64
	,Amount NUMERIC(18,2) encode az64
	,CurrencySign VARCHAR(5) encode zstd
	,CurrencyCode VARCHAR(5) encode zstd
	,TransactionStatus INT encode az64
	,TransactionDate DATETIME encode az64
	,TimeOnFile VARCHAR(50) encode zstd
	,Email VARCHAR(50) encode zstd
	,MaskedCardNumber VARCHAR(50) encode zstd
	,ParentTransId VARCHAR(30) encode zstd
	,Totalamount NUMERIC(18,2) encode az64
	,Refundamount NUMERIC(18,2) encode az64
	,Refundstatus VARCHAR(10) encode zstd
	,failedorderid VARCHAR(20) encode zstd
	,failedMerchantcode VARCHAR(30) encode zstd
	,RefundReason VARCHAR(100) encode zstd
	,batch_id INTEGER
	,created_DATE TIMESTAMP
	,rec_checksum VARCHAR(32)
);

CREATE TABLE uk_rrbs_dm.tgl_tble_shop_transaction_status_duplcdr
(
    Id INT encode az64
	,TransactionId VARCHAR(30) encode zstd
	,SubscriberId VARCHAR(20) encode zstd
	,CardNumber VARCHAR(200) encode zstd
	,CustomerIPAddress VARCHAR(20) encode zstd
	,OrderId VARCHAR(20) encode zstd
	,CountryId INT encode az64
	,BrandId INT encode az64
	,ChannelId INT encode az64
	,GatewayId INT encode az64
	,FeatureId INT encode az64
	,Amount NUMERIC(18,2) encode az64
	,CurrencySign VARCHAR(5) encode zstd
	,CurrencyCode VARCHAR(5) encode zstd
	,TransactionStatus INT encode az64
	,TransactionDate DATETIME encode az64
	,TimeOnFile VARCHAR(50) encode zstd
	,Email VARCHAR(50) encode zstd
	,MaskedCardNumber VARCHAR(50) encode zstd
	,ParentTransId VARCHAR(30) encode zstd
	,Totalamount NUMERIC(18,2) encode az64
	,Refundamount NUMERIC(18,2) encode az64
	,Refundstatus VARCHAR(10) encode zstd
	,failedorderid VARCHAR(20) encode zstd
	,failedMerchantcode VARCHAR(30) encode zstd
	,RefundReason VARCHAR(100) encode zstd
	,batch_id INTEGER
	,created_DATE TIMESTAMP
	,rec_checksum VARCHAR(32)
);