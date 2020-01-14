CREATE OR REPLACE PROCEDURE uk_rrbs_dm.sp_rrbs_dimload(p_filename "varchar",p_dim_tablename "varchar",p_src_sys "varchar")
	LANGUAGE plpgsql
AS $$ 	 	 	
        /*
        p_src_sys values e.g., RRBS-Voice-CdrType , RRBS-Data-CdrType
        */
        DECLARE
                v_filename VARCHAR(100);
                v_batchid int4 default 0;
                v_complete_status   VARCHAR(20) DEFAULT 'Complete';
                v_errm              VARCHAR(100);
                v_file_count        INT;
                v_file_ctrl_success INT;
                v_check_stage       INT;
               	v_stage_rowCount		INT;
               	v_stage_id_rowCount	INT;
                v_check_dim        INT;
                dim_insert_count  INT;
               	dim_update_count  INT;
                v_rec_notfound     VARCHAR(100) DEFAULT 'Records not found in Stage';
                v_dummy_1          VARCHAR(100);
                v_dummy_2          VARCHAR(100);
                v_exec_sql         VARCHAR(1000);
                v_stage_table_name VARCHAR(50) ;--DEFAULT 'stg_rrbs_dim_generic_test';
                v_dim_table_name   VARCHAR(50) ;--DEFAULT 'test_dim_rrbs_sms_cdrtype';
                v_dim_col_id       VARCHAR(50);--dim table id column name
                v_dim_col_val      VARCHAR(50);--dim table val column name
                v_stage_schema     VARCHAR(20) DEFAULT 'uk_rrbs_stg';
                v_dm_schema        VARCHAR(20) DEFAULT 'uK_rrbs_dm';
                sql1               VARCHAR(1000);
                sql2               VARCHAR(1000);
               sql3               VARCHAR(1000);
        BEGIN
                SET search_path TO uk_rrbs_dm;
                RAISE NOTICE 'Procedure body begins here';
                v_filename       = p_filename;
                v_dim_table_name = p_dim_tableName;--'dm.dim_rrbs_sms_cdrtype';
                
               
               /*Derive the Stage table name from the Dim table*/                
                select table_name into v_stage_table_name from information_schema.tables where table_name like 'stg%'
                and substring (v_dim_table_name,5) = substring(table_name,5);
                raise notice 'v_stage_table_name: %',v_stage_table_name;
                
				
                /* fetching the dim column id, & dim coloumn value*/
               /* sql1 = 'select  "column"  from pg_catalog.pg_table_def where tablename = '
                || QUOTE_LITERAL(v_dim_table_name)
                || ' and "column" like ''%_id'' '
--				||QUOTE_IDENT('%_id') 
				||' and "column"<>'
				||QUOTE_LITERAL('batch_id')*/
				
				 sql1 = 'select  "column"  from pg_catalog.pg_table_def where tablename = '
                || QUOTE_LITERAL(v_dim_table_name)
                || ' and "column" like ''%_id'' '
				||' and "column"<>'
				||QUOTE_LITERAL('batch_id')
				|| ' and "column" not like ''sk_%'' '
			;
			
                EXECUTE sql1 INTO v_dim_col_id;
                raise notice 'sql1 : %' ,sql1;
                
                sql2 = 'select  "column" from pg_catalog.pg_table_def where tablename =
		'
                || QUOTE_LITERAL(v_dim_table_name)
                || ' and "column" like ''%_val'' ';
                raise notice 'sql2 : %' ,sql2;
                EXECUTE sql2 INTO v_dim_col_val;
               
               
                raise notice 'v_dim_table_name - % ' ,v_dim_table_name;
                raise notice 'cold id - % ' ,v_dim_col_id;
                raise notice 'cold val - % ' ,v_dim_col_val;
               
               
                /* Check in stage table --> if the records for this filename exists  */
                sql1 = 'select count(1) from '
                || QUOTE_IDENT(v_stage_schema)
                || '.'
                || QUOTE_IDENT(v_stage_table_name)
                || ' where filename = '|| QUOTE_LITERAL(v_filename);
                EXECUTE sql1 INTO v_check_stage;
                raise notice 'v_check_stage: %' ,v_check_stage;
               
               sql2 = 'select count(1) from '
                || QUOTE_IDENT(v_stage_schema)
                || '.'
                || QUOTE_IDENT(v_stage_table_name)
                ;
                EXECUTE sql2 INTO v_stage_rowCount;
                raise notice 'v_stage_rowCount: %' ,v_stage_rowCount;
               
               sql3 = 'select max (ct) from ( select count(1) ct from '
                || QUOTE_IDENT(v_stage_schema)
                || '.'
                || QUOTE_IDENT(v_stage_table_name)
                || ' group by param_id ) A'
                ;
                EXECUTE sql3 INTO v_stage_id_rowCount;
                raise notice 'v_stage_rowCount: %' ,v_stage_id_rowCount;
               
               ----------------------------------------------------
                
               
               BEGIN
                        IF v_check_stage = 0 THEN
                                RAISE NOTICE 'File Name ''%'' not available in Stage table' ,v_filename;
                        ELSEIF    v_stage_rowCount <> 	v_check_stage THEN
                        		RAISE NOTICE 'Please load only one file in stage table at a time, multiple files detected';
                        ELSEIF  v_stage_id_rowCount >1 THEN
                        		RAISE NOTICE 'Please load only one row per param_id';
                                --   	
                        ELSE
                                --        --BEGIN /* Check in Ctrl table --> if the file has not been successfully loaded in Fact table */
                                RAISE NOTICE 'File Name ''%'' is available in Stage table' ,v_filename;
                                RAISE NOTICE '.. Checking in ctrl table if the file has already been loaded in Dim table';
                                --   
                                
                               /*Check the file status in the Control Table */
                                SELECT
                                        COUNT(1)
                                INTO
                                        v_file_ctrl_success
                                FROM
                                		  uk_rrbs_dm.ctrl_rrbs_dim
--                                        dm.test_rrbs_dim_ctrl
                                WHERE
                                        filename    = v_filename
                                AND     load_status = v_complete_status;
                                
                                raise notice 'v_file_ctrl_success : %' ,v_file_ctrl_success;
                                --

                               /*Check if this filename uploaded in Dim table*/ 
                               sql1 = 'select top 1 batch_id  from '
                                || QUOTE_IDENT(v_dm_schema)
                                || '.'
                                || QUOTE_IDENT(v_dim_table_name)
--                                || ' where batch_id IN ( select batch_id from dm.test_rrbs_dim_ctrl where filename = '
                                || ' where batch_id IN ( select batch_id from uk_rrbs_dm.ctrl_rrbs_dim where filename = '
                                || QUOTE_LITERAL(v_filename)
                                || ' and load_status = '
                                || QUOTE_LITERAL(v_complete_status)
                                || ')';
                                raise notice 'sql 1 line 108: %' ,sql1;
								execute sql1 into v_dummy_2;
							
                                GET DIAGNOSTICS v_check_dim := ROW_COUNT;
                                raise notice 'v_check_dim : %' ,v_check_dim;
                               
                               
                                IF ( v_file_ctrl_success > 0 AND v_check_dim > 0 ) 
                                THEN
                                        RAISE NOTICE 'File found to be already loaded in Dim Table';
                                ELSIF (v_file_ctrl_success <> v_check_dim) THEN
                                        RAISE NOTICE 'File discrepency between CTRL & Dim table';
                                ELSE
                                        RAISE NOTICE 'File has not been loaded in Dim Table';
/*If record exists in Stage table then Derive the Batch-Id for the Control table & insert new batch record in the control table */
                                        BEGIN
                                                IF v_check_stage > 0 THEN
                                                        RAISE NOTICE 'File exists in Stage table, Inserting batch id in Control table';  	
                                                        SELECT
                                                                COALESCE(MAX(batch_id), 0) + 1
                                                        INTO
                                                                v_batchid
                                                        FROM
                                                        		uk_rrbs_dm.ctrl_rrbs_dim
--                                                                dm.test_rrbs_dim_ctrl /* to be changed */
                                                                ;
                                                        --          /*Insert record in control table*/
                                                        CALL uk_rrbs_dm.sp_rrbs_cntrlDim_insert(v_filename, v_batchid, p_src_sys, v_check_stage) ;                                                        
                                                        RAISE NOTICE 'Batch-id and Filename inserted in Control table - %' ,v_batchid;
                                                END IF;
                                        END;
                                        --    
                                           	
           								BEGIN	
            							RAISE NOTICE 'Inserting Records into dim table';
/* update in dim table  - close the existing records with matching id */
                                        
                                        sql1 = 'UPDATE '
                                        || QUOTE_IDENT(v_dm_schema)
                                        || '.'
                                        || QUOTE_IDENT(v_dim_table_name)
                                        || '  SET is_recent = 0, valid_to = sysdate where '                                        
                                        || QUOTE_IDENT(v_dim_col_id)
                                        || ' = (select max(param_id) from '
                                        || QUOTE_IDENT(v_stage_schema)
                                        || '.'
                                        || QUOTE_IDENT(v_stage_table_name)
                                        || ' b where '
                                        ||QUOTE_IDENT(v_dm_schema)
                                        || '.'
                                        || QUOTE_IDENT(v_dim_table_name)
                                        || '.'
                                        || QUOTE_IDENT(v_dim_col_id)
                                        || ' =b.param_id and '
                                        ||QUOTE_IDENT(v_dm_schema)
                                        || '.'
                                        || QUOTE_IDENT(v_dim_table_name)
                                        || '.'
                                        || QUOTE_IDENT(v_dim_col_val)
                                        || '<> b.param_val )';
                                        raise NOTICE 'sql 1 line 151 %' ,sql1;
                                    EXECUTE sql1;
                                    GET DIAGNOSTICS dim_update_count := ROW_COUNT;
                                    RAISE NOTICE 'Rows updated in Dimension table = %' ,dim_update_count;
                                        
/* Insert the new or the changed records into the Dim table */                                       
                                   
sql1 =                                  'insert into '
                                        || QUOTE_IDENT(v_dm_schema)
                                        || '.'
                                        || QUOTE_IDENT(v_dim_table_name)
                                        || '  ( '
                                        || --dm.dim_rrbs_sms_cdrtype p
                                        --(cdrtype_id, x.cdrtype_val, x.param_desc,vaid_from,valid_to,is_recent,batch_id)
                                        QUOTE_IDENT(v_dim_col_id)
                                        || ','
                                        || QUOTE_IDENT(v_dim_col_val)
                                        || ','
                                        || 'param_desc, valid_from, valid_to, is_recent, batch_id ) '
                                        ||'select z.param_id, z.param_val, z.param_desc, sysdate, ' 
                                        ||QUOTE_LITERAL('9999-12-31')
                                        || ' ,1,'
                                        ||QUOTE_LITERAL (v_batchid)
                                        ||' from  ( select stg.param_id, stg.param_val, stg.param_desc from '
                                        || QUOTE_IDENT(v_stage_schema)
                                        || '.'
                                        || QUOTE_IDENT(v_stage_table_name)
                                        || ' stg LEFT JOIN '
                                        || QUOTE_IDENT(v_dm_schema)
                                        || '.'
                                        || QUOTE_IDENT(v_dim_table_name)
                                        || ' dim ON dim.'
                                        || QUOTE_IDENT(v_dim_col_id)
                                        || ' = stg.param_id and dim.'
                                        || QUOTE_IDENT(v_dim_col_val)
                                        || ' = stg.param_val where dim.'
                                        || QUOTE_IDENT(v_dim_col_id)
                                        || ' is null ) z'
                                       ;
                                        raise NOTICE 'sql 1 line 170 %' ,sql1;
                                        execute sql1 ;
                                        /* Take count of records inserted into Dim table */
                                        GET DIAGNOSTICS dim_insert_count := ROW_COUNT;
                                        RAISE NOTICE 'Rows inserted in Dimension table = %' ,dim_insert_count;
           END;
                                        --      	
/* Update Control table after successful insert into Fact table */
           BEGIN 
             IF dim_insert_count > 0 THEN --FOUND then
             RAISE NOTICE 'Update success status & row count in ctrl table';
               --5.
               CALL uk_rrbs_dm.sp_rrbs_cntrlDim_update(v_batchid, v_complete_status, dim_insert_count);
             RAISE NOTICE 'End of Control table update - 1';
             END IF;
           END;
                                        --      	
/* Update Control table when no records found in stage table / Redundant check*/  
           BEGIN       	
             IF dim_insert_count = 0 THEN
             RAISE NOTICE 'Update No-record status in control table with batchid = %', v_batchid;
               CALL --dm.sp_factCntrl_upd
			   uk_rrbs_dm.sp_rrbs_cntrlDim_update (v_batchid, v_rec_notfound, 0);
             RAISE NOTICE 'End of Control table update - 2';
             END IF;                  	
           END;
                                        --    	
                                END IF;
                                ----END;
                                --
                                --      	
                        END IF;
                END;
                RAISE INFO 'Process for filename % Completed' ,v_filename;
        EXCEPTION
        WHEN OTHERS THEN
                RAISE NOTICE 'error message SQLERRM %' ,SQLERRM;
                RAISE NOTICE 'error message SQLSTATE %' ,SQLSTATE;
        END;
           $$
;

