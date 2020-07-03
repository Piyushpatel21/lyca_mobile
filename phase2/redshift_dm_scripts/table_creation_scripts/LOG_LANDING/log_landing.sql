CREATE TABLE IF NOT EXISTS log_landing
(
	file_id BIGINT  DEFAULT default_identity(718122, 0, '1,1'::text) ENCODE az64
	,source_system VARCHAR(100)   ENCODE lzo
	,filename_orig VARCHAR(100)   ENCODE lzo
	,target_system VARCHAR(100)   ENCODE lzo
	,filename_target VARCHAR(100)   ENCODE lzo
	,file_landing_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,file_reg_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,file_etag VARCHAR(50)   ENCODE lzo
	,file_size INTEGER   ENCODE az64
	,file_source VARCHAR(20)   ENCODE lzo
	,status CHAR(20)   ENCODE lzo
)
DISTSTYLE KEY
 DISTKEY (file_id)
 SORTKEY (
	file_landing_date, filename_target
	);