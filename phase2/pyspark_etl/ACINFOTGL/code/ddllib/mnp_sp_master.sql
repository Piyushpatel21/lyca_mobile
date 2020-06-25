CREATE TABLE uk_rrbs_dm.tgl_mnp_sp_master
(
    ID BIGINT encode az64
	,ONOCode VARCHAR(5)
	,SPCode VARCHAR(5)
	,SPDesc VARCHAR(50)
	,STATUS INT encode az64
	,LastModifiedBy VARCHAR(50)
	,LastModifiedDate DATETIME encode az64
	,batch_id INTEGER
	,created_DATE TIMESTAMP
	,rec_checksum VARCHAR(32)
);

CREATE TABLE uk_rrbs_dm.tgl_mnp_sp_master_duplcdr
(
    ID BIGINT encode az64
	,ONOCode VARCHAR(5)
	,SPCode VARCHAR(5)
	,SPDesc VARCHAR(50)
	,STATUS INT encode az64
	,LastModifiedBy VARCHAR(50)
	,LastModifiedDate DATETIME encode az64
	,batch_id INTEGER
	,created_DATE TIMESTAMP
	,rec_checksum VARCHAR(32)
);