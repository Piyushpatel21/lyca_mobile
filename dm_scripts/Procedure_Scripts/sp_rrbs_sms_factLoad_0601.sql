CREATE OR REPLACE PROCEDURE uk_rrbs_dm.sp_rrbs_sms_factLoad(p_filename "varchar")
	LANGUAGE plpgsql
AS $$ 	 	 	
        DECLARE
                v_filename 			VARCHAR(100);
                v_batchid 			int4;                
                v_complete_status 	VARCHAR(20) DEFAULT 'Complete';
                v_errm            	VARCHAR(100);
				v_file_count	  	INT;
				v_file_ctrl_success INT;
				v_check_stage_count	INT;
				v_check_fact		INT;
				fact_insert_count 	INT;
				v_rec_notfound	  	VARCHAR(100) DEFAULT 'Zero records loaded in Fact';
				--v_dummy_1 			VARCHAR(100);
				v_dummy_2 			VARCHAR(100);
				
        BEGIN		
			RAISE NOTICE 'Procedure body begins here';                        
			v_filename = p_filename; 
			
				BEGIN	/*	Check in stage table --> if the records for this filename exists 	*/
						 /*SELECT top 1 filename into v_dummy_1 FROM stg.stage_abc 
						WHERE filename = v_filename ;
						GET DIAGNOSTICS v_check_stage_count := ROW_COUNT;*/
						SELECT COUNT(1) INTO v_check_stage_count FROM stg.stg_rrbs_uk_sms WHERE filename = v_filename ;
				END;
				
				BEGIN
					IF v_check_stage_count = 0 THEN 
						RAISE NOTICE 'File Name ''%'' not available in Stage table',	v_filename;
					
				
					ELSE
								--BEGIN	/*	Check in Ctrl table --> if the file has not been successfully loaded in Fact table	*/
								RAISE NOTICE 'File Name ''%'' is available in Stage table',	v_filename;
								RAISE NOTICE '.. Checking in ctrl table & Fact table if the file has already been loaded in Fact table'     ;
									
								SELECT COUNT(1) INTO v_file_ctrl_success FROM uk_rrbs_dm.ctrl_rrbs_fact

								WHERE
										filename = v_filename
								AND     load_status = v_complete_status ;

								SELECT top 1 filename into v_dummy_2 FROM uk_rrbs_dm.fact_rrbs_sms WHERE filename = v_filename ;
								GET DIAGNOSTICS v_check_fact := ROW_COUNT;
									
							IF (v_file_ctrl_success > 0 AND v_check_fact > 0) THEN 
									RAISE NOTICE 'File found to be already loaded in Fact Table';

							ELSIF (v_file_ctrl_success <> v_check_fact ) THEN
									RAISE NOTICE 'File discrepency between CTRL & FACT tables';
							ELSE										
									RAISE NOTICE 'File has not been loaded in Fact Table';						
									/*If record exists in Stage table then Derive the max Batch-Id from Control table & insert new batch record in control table	*/
									BEGIN
									IF v_check_stage_count > 0 THEN                                              
									RAISE NOTICE 'File exists in Stage table, Inserting batch id in Control table';
									
										SELECT COALESCE (MAX(batch_id) , 0) +1 INTO v_batchid FROM uk_rrbs_dm.ctrl_rrbs_fact;													
										/*Insert record in control table*/
										CALL 
										uk_rrbs_dm.sp_rrbs_cntrlFact_insert (v_filename, v_batchid, 'RRBS-SMS', v_check_stage_count )		;--(p_filename "varchar",p_batchid "int4", p_srcSys "varchar", p_stgCount "int4");
										RAISE NOTICE 'Batch-id, Filename, Stage-Count inserted in Control table';
									END IF;
									END;																																
								
									BEGIN	
									RAISE NOTICE 'Inserting Records into fact table';
												INSERT INTO
												uk_rrbs_dm.fact_rrbs_sms														
												(
														cdr_types													
	,network_id													
	,call_type													
	,plan_id												
	,service_id												
	,cli							
	,dialed_number							
	,imsi												
	,serving_node												
	,destination_area_code						
	,destination_zone_code						
	,destination_zone_name						
	,roam_flag						
	,roaming_node						
	,roaming_area_code						
	,roaming_zone_name						
	,sms_feature						
	,number_of_sms_charged						
	,number_of_free_sms						
	,initial_account_balance							
	,msg_cost							
	,balance						
	,free_sms_account_balance													
	,instance_id_session_id												
	,msg_date						
	,account_id					
	,dept_id					
	,free_zone_id						
	,last_topup_type						
	,bundle_code						
	,free_zone_expiry_date						
	,bundle_sms_charge						
	,bundle_balance						
	,bundle_plan_id						
	,nsms_free_units						
	,local_roam_country_code						
	,cdr_time_stamp						
	,bucket_type						
	,initial_free_units						
	,subscriber_type						
	,sub_acct_id						
	,family_id						
	,concat_message_flag					
	,concat_message_reference_number												
	,concat_message_total_chunks												
	,concat_message_current_chunk												
	,trace_id				
	,idp_time								
	,extension_record								
	,call_indicator								
	,serving_node_name								
	,bundle_version_name								
	,flh_reward_slab_id								
	,bundle_version_id								
	,rate_id								
	,pricing_plan_id								
	,pocket_id								
	,consumed_promo_amount								
	,rlh_charging_indicator								
	,surcharge_consumed								
	,roaming_partner_id								
	,filename
	,batch_id
	,created_date	                   
														)
												SELECT
		NULLIF(cdr_types,'')::SMALLINT	
,NULLIF(network_id,'')::SMALLINT	
,NULLIF(call_type,'')::SMALLINT	
,plan_id  
,service_id::SMALLINT	
,NULLIF(cli,'')::BIGINT	
,dialed_number--	VARCHAR(50)	
,NULLIF(imsi,'')::	BIGINT	
,NULLIF(serving_node,'')::	BIGINT	
,NULLIF(destination_area_code,''):: INTEGER 
,NULLIF(destination_zone_code,''):: INTEGER 
,destination_zone_name--	VARCHAR(50)	
,NULLIF(roam_flag,''):: INTEGER 
,NULLIF(roaming_node,''):: INTEGER 
,NULLIF(roaming_area_code,''):: INTEGER 
,roaming_zone_name	--VARCHAR(50)	
,NULLIF(sms_feature,''):: INTEGER 
,NULLIF(number_of_sms_charged,''):: INTEGER 
,NULLIF(number_of_free_sms,''):: INTEGER 
,initial_account_balance::	DECIMAL(10,4)	
,msg_cost::	DECIMAL(10,4)	
,balance::	DECIMAL(10,4)	
,NULLIF(free_sms_account_balance,''):: INTEGER 
,instance_id_session_id	--VARCHAR(20)	
,to_timestamp(msg_date, 'YYYYMMDDHHMISS')	
,account_id:: INTEGER 
,dept_id:: INTEGER 
,NULLIF(free_zone_id,''):: INTEGER 
,NULLIF(last_topup_type,''):: SMALLINT 
,NULLIF(bundle_code,''):: INTEGER
,to_date(free_zone_expiry_date,'dd-mm-yyyy')	
,NULLIF(bundle_sms_charge,''):: decimal(10,4) 
,NULLIF(bundle_balance,'')::	DECIMAL(10,4)	
,NULLIF(bundle_plan_id,''):: INTEGER 
,NULLIF(nsms_free_units,''):: INTEGER 
,NULLIF(local_roam_country_code,''):: SMALLINT 
,to_timestamp(cdr_time_stamp, 'YYYYMMDDHHMISS'	)
,NULLIF(bucket_type,'')::	SMALLINT	
,NULLIF(initial_free_units,''):: decimal(20,4) 
,NULLIF(subscriber_type,'')::	SMALLINT	
,NULLIF(sub_acct_id,''):: BIGINT 
,family_id	--VARCHAR(50)	
,NULLIF(concat_message_flag,'')::	SMALLINT	
,NULLIF(concat_message_reference_number,''):: INTEGER 
,NULLIF(concat_message_total_chunks,''):: INTEGER 
,NULLIF(concat_message_current_chunk,''):: INTEGER 
,trace_id	--VARCHAR(50)	
,idp_time	--VARCHAR(50)	
,NULLIF(extension_record,'')::	SMALLINT	
,NULLIF(call_indicator,'')::	SMALLINT	
,serving_node_name	--VARCHAR(50)	
,bundle_version_name	--VARCHAR(50)	
,NULLIF(flh_reward_slab_id,''):: INTEGER 
,NULLIF(bundle_version_id,'')::	SMALLINT	
,NULLIF(rate_id,''):: INTEGER 
,NULLIF(pricing_plan_id,'')::	SMALLINT	
,NULLIF(pocket_id,'')::	BIGINT	
,NULLIF(consumed_promo_amount,'')::	DECIMAL(10,4)	
,NULLIF(rlh_charging_indicator,'')::	SMALLINT	
,surcharge_consumed	--VARCHAR(50)	
,roaming_partner_id	--VARCHAR(50)	
,FILENAME
,v_batchid
,CREATED_DATE        
												FROM
        										stg.stg_rrbs_uk_sms srus														
												WHERE
												FILENAME = v_filename;
										/*	Take count of records inserted into Fact table	*/        
												GET DIAGNOSTICS fact_insert_count := ROW_COUNT;
												RAISE NOTICE 'Rows inserted in fact table = %', fact_insert_count;									
										END;
							
							
										BEGIN	/*	Update Control table after successful insert into Fact table	*/                        
													IF fact_insert_count > 0 THEN --FOUND then
													RAISE NOTICE 'Update success status & row count in ctrl table';
															--5.
														CALL uk_rrbs_dm.sp_rrbs_cntrlFact_update(v_batchid, v_complete_status, fact_insert_count);
													RAISE NOTICE 'End of Control table update - 1';
													END IF;
										END;
							
							
											BEGIN	--Update Control table when no records loaded in Fact									
													IF fact_insert_count = 0 THEN
													RAISE NOTICE 'Update No-record status in control table with batchid = %', v_batchid;
															CALL uk_rrbs_dm.sp_rrbs_cntrlFact_update(v_batchid, v_rec_notfound, 0);
													RAISE NOTICE 'End of Control table update - 2';
													END IF;																			
											END;
					
							END IF; 
							
					END IF;					
            
				END;

			RAISE INFO 'Process for filename % Completed' , v_filename;
				
        EXCEPTION
        WHEN OTHERS THEN
                RAISE NOTICE 'error message SQLERRM %', SQLERRM;
                RAISE NOTICE 'error message SQLSTATE %', SQLSTATE;
        END;
        
		   $$
;
