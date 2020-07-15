CREATE OR REPLACE PROCEDURE uk_logs.sp_create_batch_acinfotgl(p_rundate timestamp,p_batch_count numeric,p_source varchar,p_schema varchar)
	LANGUAGE plpgsql
AS $$
	
	

DECLARE
	--                v_filename 			VARCHAR(100);
	--v_timezone			varchar(10);
	v_rundate TIMESTAMP;

v_run_hour SMALLINT;

v_start_hour TIMESTAMP;

v_batch_count_a_day SMALLINT;

batch_duration SMALLINT;

v_start_time TIMESTAMP;

v_end_time TIMESTAMP;

v_check_batch INT;

v_start_offset INT;--timestamp;

sql1 VARCHAR(500);

sql2 VARCHAR(500);

insert_count INT;

v_source VARCHAR(100);

v_max_batchid BIGINT;

v_schema VARCHAR(20);

v_sql1 VARCHAR(500);

BEGIN
	v_schema = p_schema;

	--set search_path to uk_test;
	v_sql1 = 'set search_path to ' || QUOTE_IDENT(v_schema);

	EXECUTE v_sql1;

	--RAISE NOTICE 'Procedure body begins here';                        
	SELECT nvl(max(batch_id), 0)
	INTO v_max_batchid
	FROM log_batch_files_acinfotgl;--log_batch_files_rrbs;

	/*v_sql1 = 'select nvl(max(batch_id),0) into v_max_batchid from '
			||QUOTE_IDENT(p_schema) ||'.'
			||QUOTE_IDENT(p_tablename)
			;
*/
	v_max_batchid = v_max_batchid + 1;

	RAISE NOTICE 'v_max_batchid %'
		,v_max_batchid;

	v_source = p_source;

	v_batch_count_a_day = p_batch_count;

	batch_duration = 24 / v_batch_count_a_day;

	--raise notice 'batch_duration : %' ,batch_duration;			
	v_rundate = p_rundate;

	SELECT extract(hour FROM v_rundate)
	INTO v_run_hour;

	raise notice 'v_run_hour : %'
		,v_run_hour;

	v_start_offset = mod(v_run_hour, batch_duration);

	raise notice 'v_start_offset : %'
		,v_start_offset;

	v_start_time = dateadd(hour, v_run_hour - v_start_offset, trunc(v_rundate));

	raise notice 'v_start_time : %'
		,v_start_time;

	v_end_time = dateadd(millisecond, - 1, dateadd(hour, batch_duration, v_start_time));

	--raise notice 'v_end_time : %' ,v_end_time;
	--check if the same start time is available in the batch_file table                               		
	SELECT COUNT(1)
	INTO v_check_batch
	FROM log_batch_files_acinfotgl--log_batch_files_rrbs--_new --log_batch_files_rrbs 
	WHERE batch_from >= v_start_time --to_timestamp(v_start_time, 'yyyy-mm-dd h24:mi:ss') 
		AND is_valid = 'Y'
		--to_timestamp(NULLIF(NULLIF(batch_from, '0'), ''), 'yyyymmddhhmiss') => v_start_time										
		AND file_source LIKE 'landing/' || v_source;

	raise notice 'v_check_batch : %'
		,v_check_batch;

	BEGIN
		IF v_check_batch > 0 THEN RAISE NOTICE 'Batch ''%'' already created'
			,v_start_time;ELSE
			lock log_batch_files_acinfotgl;--log_batch_files_rrbs;

		--insert
		INSERT INTO log_batch_files_acinfotgl--log_batch_files_rrbs --_new--log_batch_files_rrbs_1
			(
			batch_id
			,file_source
			,file_id
			,file_name
			,target_system
			,batch_from
			,batch_to
			,is_valid
			,batch_createtime
			)
		SELECT v_max_batchid
			,source_system
			,--file_source,
			file_id
			,filename_target
			,target_system
			,v_start_time
			,v_end_time
			,'Y'
			,sysdate
		FROM log_landing
		WHERE source_system LIKE 'landing/' || v_source
			AND file_landing_date >= v_start_time --to_timestamp(v_start_time, 'yyyy-mm-dd h24:mi:ss')
			AND file_landing_date < v_end_time --to_timestamp(v_end_time, 'yyyy-mm-dd h24:mi:ss')
			;

		GET DIAGNOSTICS insert_count := ROW_COUNT;

		RAISE NOTICE 'Rows inserted in fact table = %'
			,insert_count;
	END

	IF ;END;END;

$$
;
