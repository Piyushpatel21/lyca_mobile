create table if not exists uk_rrbs_dm.tgl_trn_activation
(
    facevalue	numeric(22,6) encode az64
    ,sellprice	varchar(50) encode zstd
    ,vat	varchar(50) encode zstd
    ,discount	numeric(22,6) encode az64
    ,netprice	varchar(50) encode zstd
    ,validity	varchar(50) encode zstd
    ,InvoiceNumber	varchar(50) encode zstd
    ,submitby	varchar(50) encode zstd
    ,submitdate	timestamp encode az64
    ,crcheckby	varchar(50) encode zstd
    ,crcheckdate	timestamp encode az64
    ,preactby	varchar(50) encode zstd
    ,preactdate	timestamp encode az64
    ,authby	varchar(50) encode zstd
    ,authdate	timestamp encode az64
    ,Blockby	varchar(50) encode zstd
    ,Blockdate	varchar(50) encode zstd
    ,blockreason	varchar(50) encode zstd
    ,HuaweiSubmitState	varchar(50) encode zstd
    ,HuaweiErrCode	varchar(50) encode zstd
    ,RecordLock	varchar(50) encode zstd
    ,Processid	varchar(50) encode zstd
    ,FreeMin	numeric(22,6) encode az64
    ,PONumber	varchar(50) encode zstd
    ,BundleCode	int encode az64
    ,PlanID	varchar(50) encode zstd
    ,Description	varchar(50) encode zstd
    ,BundleCost	numeric(22,6) encode az64
    ,BundleText	varchar(50) encode zstd
    ,Total_Fee_Amt	numeric(22,6) encode az64
    ,Fee_Breakup	varchar(50) encode zstd
    ,ISFAMILY	varchar(50) encode zstd
    ,SIM_ExpiryDate	date encode az64
    ,authdate_month	int encode az64
    ,batch_id	INTEGER encode az64
    ,created_DATE	TIMESTAMP encode az64
    ,rec_checksum	VARCHAR(32) encode zstd
);

create table if not exists uk_rrbs_dm.tgl_trn_activation_duplcdr
(
    facevalue	numeric(22,6) encode az64
    ,sellprice	varchar(50) encode zstd
    ,vat	varchar(50) encode zstd
    ,discount	numeric(22,6) encode az64
    ,netprice	varchar(50) encode zstd
    ,validity	varchar(50) encode zstd
    ,InvoiceNumber	varchar(50) encode zstd
    ,submitby	varchar(50) encode zstd
    ,submitdate	timestamp encode az64
    ,crcheckby	varchar(50) encode zstd
    ,crcheckdate	timestamp encode az64
    ,preactby	varchar(50) encode zstd
    ,preactdate	timestamp encode az64
    ,authby	varchar(50) encode zstd
    ,authdate	timestamp encode az64
    ,Blockby	varchar(50) encode zstd
    ,Blockdate	varchar(50) encode zstd
    ,blockreason	varchar(50) encode zstd
    ,HuaweiSubmitState	varchar(50) encode zstd
    ,HuaweiErrCode	varchar(50) encode zstd
    ,RecordLock	varchar(50) encode zstd
    ,Processid	varchar(50) encode zstd
    ,FreeMin	numeric(22,6) encode az64
    ,PONumber	varchar(50) encode zstd
    ,BundleCode	int encode az64
    ,PlanID	varchar(50) encode zstd
    ,Description	varchar(50) encode zstd
    ,BundleCost	numeric(22,6) encode az64
    ,BundleText	varchar(50) encode zstd
    ,Total_Fee_Amt	numeric(22,6) encode az64
    ,Fee_Breakup	varchar(50) encode zstd
    ,ISFAMILY	varchar(50) encode zstd
    ,SIM_ExpiryDate	date encode az64
    ,authdate_month	int encode az64
    ,batch_id	INTEGER encode az64
    ,created_DATE	TIMESTAMP encode az64
    ,rec_checksum	VARCHAR(32) encode zstd
);

