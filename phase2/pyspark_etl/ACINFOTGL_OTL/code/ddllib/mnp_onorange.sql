CREATE TABLE uk_rrbs_dm.tgl_mnp_onorange
(
     id BIGINT encode az64
	,ONOCode VARCHAR(5)
	,Prefix BIGINT encode az64
	,STATUS INT encode az64
	,LastModifiedBy VARCHAR(50)
	,LastModifiedDate DATETIME encode az64
	,batch_id INTEGER
	,created_DATE TIMESTAMP
	,rec_checksum VARCHAR(32)
);

CREATE TABLE uk_rrbs_dm.tgl_mnp_onorange_duplcdr
(
     id BIGINT encode az64
	,ONOCode VARCHAR(5)
	,Prefix BIGINT encode az64
	,STATUS INT encode az64
	,LastModifiedBy VARCHAR(50)
	,LastModifiedDate DATETIME encode az64
	,batch_id INTEGER
	,created_DATE TIMESTAMP
	,rec_checksum VARCHAR(32)
);