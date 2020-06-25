create table if not exists uk_rrbs_dm.tgl_trn_activation_bundle_cost
(
	Activateid	int                  encode zstd
	,BundleCost	numeric(22,6)        encode az64
	,CreatedDate	timestamp            encode az64
	,BundleCode	int                  encode az64
	,planid	varchar(50)              encode zstd
	,NoofMonths	varchar(50)          encode zstd
	,Priority	varchar(50)          encode zstd
	,Isprocess	varchar(50)          encode zstd
	,BundleText	varchar(100)         encode zstd
	,Total_Fee_Amt	numeric(22,6)    encode az64
	,Fee_Breakup	varchar(100)     encode zstd
	,Family_Bundle_Type	varchar(50)  encode zstd
	,batch_id	INTEGER              encode az64
	,created_DATE	TIMESTAMP        encode az64
	,rec_checksum	VARCHAR(32)      encode zstd
);

create table if not exists uk_rrbs_dm.tgl_trn_activation_bundle_cost_duplcdr
(
	Activateid	int                  encode zstd
	,BundleCost	numeric(22,6)        encode az64
	,CreatedDate	timestamp            encode az64
	,BundleCode	int                  encode az64
	,planid	varchar(50)              encode zstd
	,NoofMonths	varchar(50)          encode zstd
	,Priority	varchar(50)          encode zstd
	,Isprocess	varchar(50)          encode zstd
	,BundleText	varchar(100)         encode zstd
	,Total_Fee_Amt	numeric(22,6)    encode az64
	,Fee_Breakup	varchar(100)     encode zstd
	,Family_Bundle_Type	varchar(50)  encode zstd
	,batch_id	INTEGER              encode az64
	,created_DATE	TIMESTAMP        encode az64
	,rec_checksum	VARCHAR(32)      encode zstd
);