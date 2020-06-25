CREATE TABLE uk_rrbs_dm.tgl_first_usage
(
    id BIGINT encode az64
	,Msisdn VARCHAR(20) encode zstd
	,imsi VARCHAR(20) encode zstd
	,IccidPrefix VARCHAR(15) encode zstd
	,Iccid VARCHAR(15) encode zstd
	,ActivationDate DATETIME encode az64
	,ResellerId VARCHAR(50) encode zstd
	,lastusagedate DATETIME encode az64
	,retailerid VARCHAR(50) encode zstd
	,DistributorId VARCHAR(50) encode zstd
	,FaceValue DECIMAL encode az64
	,processed_flag VARCHAR(50) encode zstd
	,HotspotId VARCHAR(50) encode zstd
	,AccMgrid VARCHAR(50) encode zstd
	,OffMgrid VARCHAR(50) encode zstd
	,swap_status VARCHAR(15) encode zstd
	,old_msisdn VARCHAR(15) encode zstd
	,old_activationdate DATETIME encode az64
	,CIP_ProcessedFlag INT encode az64
	,FCABundle_ProcessedFlag INT encode az64
	,firsttopupdate DATETIME encode az64
	,TopupDate DATETIME encode az64
	,retailerid_indirect VARCHAR(50) encode zstd
	,Brand VARCHAR(10) encode zstd
	,FIRST_ICCID VARCHAR(100) encode zstd
	,IccidComputed VARCHAR(15) encode zstd
	,FcaType INT encode az64
	,batch_id INTEGER
	,created_DATE TIMESTAMP
	,rec_checksum VARCHAR(32)
);

CREATE TABLE uk_rrbs_dm.tgl_first_usage_duplcdr
(
    id BIGINT encode az64
	,Msisdn VARCHAR(20) encode zstd
	,imsi VARCHAR(20) encode zstd
	,IccidPrefix VARCHAR(15) encode zstd
	,Iccid VARCHAR(15) encode zstd
	,ActivationDate DATETIME encode az64
	,ResellerId VARCHAR(50) encode zstd
	,lastusagedate DATETIME encode az64
	,retailerid VARCHAR(50) encode zstd
	,DistributorId VARCHAR(50) encode zstd
	,FaceValue DECIMAL encode az64
	,processed_flag VARCHAR(50) encode zstd
	,HotspotId VARCHAR(50) encode zstd
	,AccMgrid VARCHAR(50) encode zstd
	,OffMgrid VARCHAR(50) encode zstd
	,swap_status VARCHAR(15) encode zstd
	,old_msisdn VARCHAR(15) encode zstd
	,old_activationdate DATETIME encode az64
	,CIP_ProcessedFlag INT encode az64
	,FCABundle_ProcessedFlag INT encode az64
	,firsttopupdate DATETIME encode az64
	,TopupDate DATETIME encode az64
	,retailerid_indirect VARCHAR(50) encode zstd
	,Brand VARCHAR(10) encode zstd
	,FIRST_ICCID VARCHAR(100) encode zstd
	,IccidComputed VARCHAR(15) encode zstd
	,FcaType INT encode az64
	,batch_id INTEGER
	,created_DATE TIMESTAMP
	,rec_checksum VARCHAR(32)
);