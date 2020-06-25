CREATE TABLE uk_rrbs_dm.tgl_tbl_ingenico_transaction_log
(
    Id INT encode az64
	,TransactionId VARCHAR(30) encode zstd
	,SubscriberId VARCHAR(20) encode zstd
	,CardNumber VARCHAR(200) encode zstd
	,cardExpiryDate DATE encode az64
	,TransactionType VARCHAR(2) encode zstd
	,OrderId VARCHAR(20) encode zstd
	,MerchantCode VARCHAR(30) encode zstd
	,PaymentMethodCode VARCHAR(30) encode zstd
	,StatusCode VARCHAR(30) encode zstd
	,ResponseCode VARCHAR(20) encode zstd
	,ResponseDescription VARCHAR(4000) encode zstd
	,CVCResultCode VARCHAR(50) encode zstd
	,AVSResultCode VARCHAR(50) encode zstd
	,paymentID VARCHAR(30) encode zstd
	,paymentRefernce VARCHAR(200) encode zstd
	,MerchantReference VARCHAR(200) encode zstd
	,PaymentMethod VARCHAR(30) encode zstd
	,paymentproductId INT encode az64
	,AuthorizationCode VARCHAR(20) encode zstd
	,fraudServiceResult VARCHAR(30) encode zstd
	,isCancelable VARCHAR(10) encode zstd
	,isAuthorized VARCHAR(10) encode zstd
	,StatusCodeChangeDateTime DATETIME encode az64
	,AdditionalReference VARCHAR(30) encode zstd
	,ExternalReference VARCHAR(30) encode zstd
	,Cavv VARCHAR(30) encode zstd
	,CavvAlgorithm VARCHAR(100) encode zstd
	,Eci VARCHAR(100) encode zstd
	,ValidationResult VARCHAR(30) encode zstd
	,Xid VARCHAR(30) encode zstd
	,IsRecurring VARCHAR(10) encode zstd
	,ReturnUrl VARCHAR(1000) encode zstd
	,Tokenid VARCHAR(200) encode zstd
	,IsnewToken VARCHAR(10) encode zstd
	,CreatedDate DATETIME encode az64
	,batch_id INTEGER
	,created_DATE TIMESTAMP
	,rec_checksum VARCHAR(32)
);

CREATE TABLE uk_rrbs_dm.tgl_tbl_ingenico_transaction_log_duplcdr
(
    Id INT encode az64
	,TransactionId VARCHAR(30) encode zstd
	,SubscriberId VARCHAR(20) encode zstd
	,CardNumber VARCHAR(200) encode zstd
	,cardExpiryDate DATE encode az64
	,TransactionType VARCHAR(2) encode zstd
	,OrderId VARCHAR(20) encode zstd
	,MerchantCode VARCHAR(30) encode zstd
	,PaymentMethodCode VARCHAR(30) encode zstd
	,StatusCode VARCHAR(30) encode zstd
	,ResponseCode VARCHAR(20) encode zstd
	,ResponseDescription VARCHAR(4000) encode zstd
	,CVCResultCode VARCHAR(50) encode zstd
	,AVSResultCode VARCHAR(50) encode zstd
	,paymentID VARCHAR(30) encode zstd
	,paymentRefernce VARCHAR(200) encode zstd
	,MerchantReference VARCHAR(200) encode zstd
	,PaymentMethod VARCHAR(30) encode zstd
	,paymentproductId INT encode az64
	,AuthorizationCode VARCHAR(20) encode zstd
	,fraudServiceResult VARCHAR(30) encode zstd
	,isCancelable VARCHAR(10) encode zstd
	,isAuthorized VARCHAR(10) encode zstd
	,StatusCodeChangeDateTime DATETIME encode az64
	,AdditionalReference VARCHAR(30) encode zstd
	,ExternalReference VARCHAR(30) encode zstd
	,Cavv VARCHAR(30) encode zstd
	,CavvAlgorithm VARCHAR(100) encode zstd
	,Eci VARCHAR(100) encode zstd
	,ValidationResult VARCHAR(30) encode zstd
	,Xid VARCHAR(30) encode zstd
	,IsRecurring VARCHAR(10) encode zstd
	,ReturnUrl VARCHAR(1000) encode zstd
	,Tokenid VARCHAR(200) encode zstd
	,IsnewToken VARCHAR(10) encode zstd
	,CreatedDate DATETIME encode az64
	,batch_id INTEGER
	,created_DATE TIMESTAMP
	,rec_checksum VARCHAR(32)
);