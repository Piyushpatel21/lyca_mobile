CREATE OR REPLACE PROCEDURE uk_rrbs_dm.sp_rrbs_cntrlDim_update(p_batchid "int4",p_status "varchar",p_rowcount "int4")
	LANGUAGE plpgsql
AS $$ 	
        DECLARE
                --V_FILENAME VARCHAR(100);
                v_batchid 	INT4;
                v_status    VARCHAR(100);
                v_rowcount  INT;
                integer_var INT;
        BEGIN
                v_batchid  = p_batchid;
                v_status   = p_status;
                v_rowcount = p_rowcount;
                raise NOTICE '		I am inside ctrl upd, batch id %', v_batchid;
                UPDATE uk_rrbs_dm.ctrl_rrbs_dim
--                        dm.test_rrbs_dim_ctrl
                        /*Change this*/
                SET
                        load_status  = v_status,
                        dimRowCount = v_rowcount,
                        load_end_time = sysdate
                WHERE
                        batch_id = v_batchid;
                
                GET DIAGNOSTICS integer_var := ROW_COUNT;
                RAISE NOTICE '		Count of Rows updated in ctrl = %', integer_var;
                /*
                EXCEPTION
                WHEN OTHERS THEN
                RAISE NOTICE 'error message SQLERRM %', SQLERRM;
                RAISE NOTICE 'error message SQLSTATE %', SQLSTATE;
                */
        END  $$
;
