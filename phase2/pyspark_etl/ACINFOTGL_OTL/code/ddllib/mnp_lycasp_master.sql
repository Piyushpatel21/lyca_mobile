CREATE TABLE uk_rrbs_dm.tgl_mnp_lycasp_master
(
    ID BIGINT encode az64
	,ONOCode VARCHAR(5)
	,SPCode VARCHAR(5)
	,SPDesc VARCHAR(50)
	,STATUS INT encode az64
	,URL VARCHAR(500)
	,UID VARCHAR(50)
	,PWD VARCHAR(50)
	,LastModifiedBy VARCHAR(50)
	,LastModifiedDate DATETIME encode az64
	,batch_id INTEGER
	,created_DATE TIMESTAMP
	,rec_checksum VARCHAR(32)
);

CREATE TABLE uk_rrbs_dm.tgl_mnp_lycasp_master_duplcdr
(
    ID BIGINT encode az64
	,ONOCode VARCHAR(5)
	,SPCode VARCHAR(5)
	,SPDesc VARCHAR(50)
	,STATUS INT encode az64
	,URL VARCHAR(500)
	,UID VARCHAR(50)
	,PWD VARCHAR(50)
	,LastModifiedBy VARCHAR(50)
	,LastModifiedDate DATETIME encode az64
	,batch_id INTEGER
	,created_DATE TIMESTAMP
	,rec_checksum VARCHAR(32)
);