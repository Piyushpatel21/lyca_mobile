CREATE OR REPLACE PROCEDURE uk_rrbs_dm.sp_rrbs_cntrlFact_update(p_batchid "int4",p_status "varchar",p_rowcount "int4")
	LANGUAGE plpgsql
AS $$ 	 	 	 	 	 
declare

--V_FILENAME VARCHAR(100); 
v_batchid int4;
v_status varchar(100);
v_rowcount int;
integer_var int;

begin

v_batchid = p_batchid;
v_status = p_status;
v_rowcount = p_rowcount;

raise NOTICE '		I am inside ctrl upd, batch id %', v_batchid ; 

update uk_rrbs_dm.ctrl_rrbs_fact
set load_status = v_status,
	factRowCount = v_rowcount
where batch_id = v_batchid;

GET DIAGNOSTICS integer_var := ROW_COUNT;
		RAISE NOTICE '		Count of Rows updated in ctrl = %', integer_var;

/*
EXCEPTION
  WHEN OTHERS THEN
     	RAISE NOTICE 'error message SQLERRM %', SQLERRM;
  		RAISE NOTICE 'error message SQLSTATE %', SQLSTATE;
*/
END
     $$
;
