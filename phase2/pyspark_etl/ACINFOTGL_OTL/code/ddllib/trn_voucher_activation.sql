CREATE TABLE uk_rrbs_dm.tgl_trn_voucher_activation
(
    voucherid INT encode az64
    ,sitecode VARCHAR(3) encode zstd
    ,card_id VARCHAR(4) encode zstd
    ,source_batch_id VARCHAR(6) encode zstd
    ,serial_fr VARCHAR(4) encode zstd
    ,serial_to VARCHAR(4) encode zstd
    ,totalcards INT encode az64
    ,resellerid VARCHAR(25) encode zstd
    ,actstate INT encode az64
    ,expiryyy INT encode az64
    ,expirymm INT encode az64
    ,expirydd INT encode az64
    ,facevalue DECIMAL(38,10) encode az64
    ,sellprice DECIMAL(38,10) encode az64
    ,vat INT encode az64
    ,discount DECIMAL(5,2) encode az64
    ,netprice DECIMAL(38,10) encode az64
    ,validity INT encode az64
    ,InvoiceNumber VARCHAR(17) encode zstd
    ,submitby VARCHAR(25) encode zstd
    ,submitdate DATETIME encode az64
    ,crcheckby VARCHAR(25) encode zstd
    ,crcheckdate DATETIME encode az64
    ,preactby VARCHAR(25) encode zstd
    ,preactdate DATETIME encode az64
    ,authby VARCHAR(25) encode zstd
    ,authdate DATETIME encode az64
    ,Blockby VARCHAR(25) encode zstd
    ,Blockdate DATETIME encode az64
    ,blockreason VARCHAR(100) encode zstd
    ,HuaweiSubmitState VARCHAR(1) encode zstd
    ,HuaweiErrCode VARCHAR(4) encode zstd
    ,RecordLock VARCHAR(1) encode zstd
    ,Processid INT encode az64
    ,PONumber VARCHAR(200) encode zstd
    ,Activated_Vouchers INT encode az64
    ,Record_Processed_Time DATETIME encode az64
    ,VoucherType INT encode az64
    ,Card_Currency VARCHAR(50) encode zstd
    ,Description VARCHAR(100) encode zstd
    ,BundleCode INT encode az64
    ,BundleCost DECIMAL(38,10) encode az64
    ,Total_Fee_Amt DECIMAL(38,10) encode az64
    ,Fee_Breakup VARCHAR(500) encode zstd
    ,batch_id INTEGER
    ,created_DATE TIMESTAMP
    ,rec_checksum VARCHAR(32)
);

CREATE TABLE uk_rrbs_dm.tgl_trn_voucher_activation_duplcdr
(
    voucherid INT encode az64
    ,sitecode VARCHAR(3) encode zstd
    ,card_id VARCHAR(4) encode zstd
    ,source_batch_id VARCHAR(6) encode zstd
    ,serial_fr VARCHAR(4) encode zstd
    ,serial_to VARCHAR(4) encode zstd
    ,totalcards INT encode az64
    ,resellerid VARCHAR(25) encode zstd
    ,actstate INT encode az64
    ,expiryyy INT encode az64
    ,expirymm INT encode az64
    ,expirydd INT encode az64
    ,facevalue DECIMAL(38,10) encode az64
    ,sellprice DECIMAL(38,10) encode az64
    ,vat INT encode az64
    ,discount DECIMAL(5,2) encode az64
    ,netprice DECIMAL(38,10) encode az64
    ,validity INT encode az64
    ,InvoiceNumber VARCHAR(17) encode zstd
    ,submitby VARCHAR(25) encode zstd
    ,submitdate DATETIME encode az64
    ,crcheckby VARCHAR(25) encode zstd
    ,crcheckdate DATETIME encode az64
    ,preactby VARCHAR(25) encode zstd
    ,preactdate DATETIME encode az64
    ,authby VARCHAR(25) encode zstd
    ,authdate DATETIME encode az64
    ,Blockby VARCHAR(25) encode zstd
    ,Blockdate DATETIME encode az64
    ,blockreason VARCHAR(100) encode zstd
    ,HuaweiSubmitState VARCHAR(1) encode zstd
    ,HuaweiErrCode VARCHAR(4) encode zstd
    ,RecordLock VARCHAR(1) encode zstd
    ,Processid INT encode az64
    ,PONumber VARCHAR(200) encode zstd
    ,Activated_Vouchers INT encode az64
    ,Record_Processed_Time DATETIME encode az64
    ,VoucherType INT encode az64
    ,Card_Currency VARCHAR(50) encode zstd
    ,Description VARCHAR(100) encode zstd
    ,BundleCode INT encode az64
    ,BundleCost DECIMAL(38,10) encode az64
    ,Total_Fee_Amt DECIMAL(38,10) encode az64
    ,Fee_Breakup VARCHAR(500) encode zstd
    ,batch_id INTEGER
    ,created_DATE TIMESTAMP
    ,rec_checksum VARCHAR(32)
);