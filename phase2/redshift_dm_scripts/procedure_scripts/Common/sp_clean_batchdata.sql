CREATE OR REPLACE PROCEDURE uk_logs.sp_clean_batchdata(p_batchid int4,p_schema varchar,p_tablename varchar)
	LANGUAGE plpgsql
AS $$
	
	
DECLARE 


v_schema varchar(50);
v_tablename varchar(50);
v_batchid integer;
v_delete_count integer;
v_sql1 varchar(500);

	BEGIN

	v_schema = p_schema ; 	
	v_tablename = p_tablename ; 
	v_batchid = p_batchid;
	
	v_sql1 = 'delete from '|| QUOTE_IDENT (v_schema)||'.'||QUOTE_IDENT (v_tablename)||' where batch_id = '||QUOTE_LITERAL ( v_batchid );	
raise notice 'v_sql1 = %', v_sql1;

	EXECUTE v_sql1 ;
	GET DIAGNOSTICS v_delete_count := ROW_COUNT;
RAISE NOTICE 'Rows deleted from table = %', v_delete_count ;	  
		 
EXCEPTION WHEN OTHERS THEN
  RAISE INFO 'error message SQLERRM %', SQLERRM;
  RAISE INFO 'error message SQLSTATE %', SQLSTATE;
		
		
	END;


$$
;
