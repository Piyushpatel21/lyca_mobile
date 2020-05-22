CREATE OR REPLACE PROCEDURE uk_rrbs_dm.sp_rrbs_cntrlDim_insert (p_filename "varchar",p_batchid "int4",p_srcsys "varchar",p_stgcount "int4")
	LANGUAGE plpgsql
AS $$ 	 	 	 	 	  
declare

V_FILENAME VARCHAR(100); 
v_batchid int4;
integer_var int;
v_srsSys VARCHAR(50);
v_stgCount int;

begin

v_filename = p_filename;
v_batchid = p_batchid;
v_srsSys = p_srcSys;
v_stgCount = p_stgCount;

 
insert into uk_rrbs_dm.ctrl_rrbs_dim
(batch_id, filename, src_sys, stgRowCount) 
--dm.test_rrbs_dim_ctrl(batch_id, filename, src_sys, stgRowCount) /*change this*/
select v_batchid, v_filename, v_srsSys, v_stgCount;

raise Notice '		I am inside ctrl insert, batch id %', v_batchid ;
GET DIAGNOSTICS integer_var := ROW_COUNT;
RAISE NOTICE '		Rows inserted in ctrl table = %', integer_var;



/*
EXCEPTION
  WHEN OTHERS THEN
     	RAISE INFO 'error message SQLERRM %', SQLERRM;
  		RAISE INFO 'error message SQLSTATE %', SQLSTATE;
*/
END
     $$
;
