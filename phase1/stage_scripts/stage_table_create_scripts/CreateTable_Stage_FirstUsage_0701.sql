CREATE TABLE uk_rrbs_stg.stg_FirstUsage(
	id bigint NOT NULL,
	Msisdn varchar(20) NULL,
	imsi varchar(20) NULL,
	IccidPrefix varchar(15) NULL,
	Iccid varchar(15) NOT NULL,
	ActivationDate datetime NOT NULL,
	ResellerId varchar(50) NULL,
	lastusagedate datetime NULL,
	retailerid varchar(50) NULL,
	DistributorId varchar(50) NULL,
	FaceValue float NULL,
	processed_flag varchar(50) NULL,
	HotspotId varchar(50) NULL,
	AccMgrid varchar(50) NULL,
	OffMgrid varchar(50) NULL,
	swap_status varchar(15) NULL,
	old_msisdn varchar(15) NULL,
	old_activationdate datetime NULL,
	CIP_ProcessedFlag int NULL CONSTRAINT DF_FirstUsage_MultiLang_CIP_ProcessedFlag  DEFAULT ((0)),
	FCABundle_ProcessedFlag int NULL CONSTRAINT DF_FirstUsage_MultiLang_FCABundle_ProcessedFlag  DEFAULT ((0)),
	firsttopupdate datetime NULL,
	TopupDate datetime NULL,
	retailerid_indirect varchar(50) NULL,
	Brand varchar(10) NULL,
	FIRST_ICCID varchar(100) NULL,
	IccidComputed varchar(15) NULL,
	FcaType int NULL
	)

diststyle even
sortkey	(ActivationDate)
