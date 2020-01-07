CREATE OR REPLACE PROCEDURE uk_rrbs_dm.sp_rrbs_gprs_factLoad(p_filename "varchar")
	LANGUAGE plpgsql
AS $$ 	 	 	
        DECLARE
                v_filename 			VARCHAR(100);
                v_batchid 			int4;                
                v_complete_status 	VARCHAR(20) DEFAULT 'Complete';
                v_errm            	VARCHAR(100);
				v_file_count	  	INT;
				v_file_ctrl_success INT;
				v_check_stage		INT;
				v_check_fact		INT;
				fact_insert_count 	INT;
				v_rec_notfound	  	VARCHAR(100) DEFAULT 'Records not found in Stage';
				v_dummy_1 			VARCHAR(100);
				v_dummy_2 			VARCHAR(100);
				
        BEGIN		
			RAISE NOTICE 'Procedure body begins here';                        
			v_filename = p_filename;
			
				BEGIN	/*	Check in stage table --> if the records for this filename exists 	*/
						 /*SELECT top 1 filename into v_dummy_1 FROM stg.stage_abc 
						WHERE filename = v_filename ;
						GET DIAGNOSTICS v_check_stage := ROW_COUNT;*/
						SELECT COUNT(1) INTO v_check_stage FROM stg.stg_rrbs_uk_gprs WHERE filename = v_filename ;
				END;
				
				BEGIN
					IF v_check_stage = 0 THEN 
						RAISE NOTICE 'File Name ''%'' not available in Stage table',	v_filename;
					
				
					ELSE
								--BEGIN	/*	Check in Ctrl table --> if the file has not been successfully loaded in Fact table	*/
								RAISE NOTICE 'File Name ''%'' is available in Stage table',	v_filename;
								RAISE NOTICE '.. Checking in ctrl table if the file has already been loaded in Fact table'     ;
									
								SELECT COUNT(1) INTO v_file_ctrl_success FROM uk_rrbs_dm.ctrl_rrbs_fact
								WHERE
										filename = v_filename
								AND     load_status = v_complete_status ;

								SELECT top 1 filename into v_dummy_2 FROM uk_rrbs_dm.fact_rrbs_gprs WHERE filename = v_filename ;
								GET DIAGNOSTICS v_check_fact := ROW_COUNT;
									
							IF (v_file_ctrl_success > 0 AND v_check_fact > 0) THEN 
									RAISE NOTICE 'File found to be already loaded in Fact Table';

							ELSIF (v_file_ctrl_success <> v_check_fact ) THEN
									RAISE NOTICE 'File discrepency between CTRL & FACT tables';
							ELSE										
									RAISE NOTICE 'File has not been loaded in Fact Table';						
								/*If record exists in Stage table then Derive the Batch-Id for the Control table & insert new batch record in the control table	*/
									BEGIN
									IF v_check_stage > 0 THEN                                              
									RAISE NOTICE 'File exists in Stage table, Inserting batch id in Control table';
									
										SELECT COALESCE (MAX(batch_id) , 0) +1 INTO v_batchid FROM uk_rrbs_dm.ctrl_rrbs_fact;--ctrl_fact;														
										/*Insert record in control table*/
										CALL --dm.sp_ctrl_ins(v_filename,v_batchid)
										uk_rrbs_dm.sp_rrbs_cntrlFact_insert (v_filename, v_batchid, 'RRBS-GPRS', v_check_stage )		;--(p_filename "varchar",p_batchid "int4", p_srcSys "varchar", p_stgCount "int4");
										RAISE NOTICE 'Batch-id and Filename inserted in Control table';
									END IF;
									END;																																
								
											BEGIN	
												RAISE NOTICE 'Inserting Records into fact table';
												INSERT INTO
														uk_rrbs_dm.fact_rrbs_gprs
														--dm.FACT_ABC --Later Change the table name here and the column list
														(
cdr_type
	,network_id
	,data_feature
	,tariffplan_id
	,service_id
	,msisdn
	,apn
	,pdp_address
	,cellid
	,imei
	,imsi
	,serving_node
	,ggsn_address
	,roaming_zone_name
	,roam_flag
	,granted_bytes_cumulative
	,granted_money
	,total_used_bytes
	,chargeable_used_bytes
	,uploaded_bytes
	,downloaded_bytes
	,data_connection_time
	,data_connection_dt
	,data_termination_time
	,time_duration
	,initial_account_balance
	,data_charge
	,final_account_balance
	,free_bytes
	,sessionid
	,last_topup_type
	,free_data_expiry_date
	,charge_indicator
	,final_unit_indicator
	,bundle_code
	,accid
	,deptid
	,bundle_used_data
	,bundle_data_charge
	,bundle_balance
	,bundle_plan_id
	,local_roam_country_code
	,sdfid
	,rategroupid
	,serviceid
	,cdr_time_stamp
	,intial_free_units
	,subscriber_type
	,cdr_sequence_number
	,sub_acct_id
	,ggsn_time
	,tariff_plan_change_id
	,used_units_before_tariff_plan_change
	,family_account_id
	,bundle_version_name
	,bundle_versionid
	,reward_slab_id
	,pricing_plan_id
	,rate_id
	,pocket_id
	,consumed_promo_amount
	,consumed_surcharge
	,rlh_charging_indicator
	,granted_bytes_current
	,roaming_partner_id
	,filename
	,batch_id
	,created_date
															                   
														)
												SELECT
		
cdr_type::SMALLINT
	,network_id::SMALLINT
	,data_feature::SMALLINT
	,tariffplan_id::SMALLINT
	,service_id::SMALLINT
	,NULLIF(msisdn, '')::BIGINT
	,apn
	,pdp_address
	,cellid
	,imei
	,nullif(imsi, '')::BIGINT
	,serving_node
	,ggsn_address
	,roaming_zone_name
	,roam_flag::SMALLINT
	,granted_bytes_cumulative::BIGINT
	,granted_money::DECIMAL(20, 6)
	,total_used_bytes::BIGINT
	,chargeable_used_bytes::BIGINT
	,uploaded_bytes::BIGINT
	,downloaded_bytes::BIGINT
	,to_timestamp(NULLIF(NULLIF(data_connection_time, '0'), ''), 'yyyymmddhhmiss') AS data_connection_time
	,to_date(SUBSTRING(NULLIF(NULLIF(data_connection_time, '0'), ''), 1, 8), 'yyyymmdd') AS data_connection_dt
	,to_timestamp(NULLIF(NULLIF(data_termination_time, '0'), ''), 'yyyymmddhhmiss')
	,NULLIF(time_duration, '')::INT
	,initial_account_balance::DECIMAL(20, 6)
	,data_charge::DECIMAL(20, 6)
	,final_account_balance::DECIMAL(20, 6)
	,NULLIF(free_bytes, '')::BIGINT
	,sessionid
	,NULLIF(last_topup_type, '')::SMALLINT
	,to_date(nullif(NULLIF(free_data_expiry_date, 0), ''), 'dd-mm-yyyy')
	,NULLIF(charge_indicator, '')::SMALLINT
	,NULLIF(final_unit_indicator, '')::SMALLINT
	,bundle_code
	,accid
	,deptid
	,bundle_used_data
	,nullif(bundle_data_charge, '')::DECIMAL(20, 6)
	,nullif(bundle_balance, '')::DECIMAL(20, 6)
	,bundle_plan_id
	,NULLIF(local_roam_country_code, '')::SMALLINT
	,NULLIF(sdfid, '')::SMALLINT
	,NULLIF(rategroupid, '')::SMALLINT
	,serviceid
	,to_timestamp(NULLIF(NULLIF(cdr_time_stamp, '0'), ''), 'yyyymmddhhmiss')
	,NULLIF(intial_free_units, '')::BIGINT
	,NULLIF(subscriber_type, '')::SMALLINT
	,NULLIF(cdr_sequence_number, '')::INT
	,NULLIF(sub_acct_id, '')::BIGINT
	,ggsn_time
	,NULLIF(tariff_plan_change_id, '')::SMALLINT
	,NULLIF(used_units_before_tariff_plan_change, '')::BIGINT
	,nullif(family_account_id, '')::INT
	,bundle_version_name
	,NULLIF(bundle_versionid, '')::SMALLINT
	,NULLIF(reward_slab_id, '')::SMALLINT
	,NULLIF(pricing_plan_id, '')::SMALLINT
	,NULLIF(rate_id, '')::INT
	,NULLIF(pocket_id, '')::INT
	,NULLIF(consumed_promo_amount, '')::DECIMAL(20, 6)
	,NULLIF(consumed_surcharge, '')::DECIMAL(20, 6)
	,NULLIF(rlh_charging_indicator, '')::SMALLINT
	,NULLIF(granted_bytes_current, '')::BIGINT
	,NULLIF(roaming_partner_id, '')::SMALLINT
	,filename
	,v_batchid
	,created_date		
       
FROM
        stg.stg_rrbs_uk_gprs sruv
														
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
							
							
											BEGIN	--Update Control table when no records found in stage table /*Redundant check*/									
													IF fact_insert_count = 0 THEN
													RAISE NOTICE 'Update No-record status in control table with batchid = %', v_batchid;
															CALL uk_rrbs_dm.sp_rrbs_cntrlFact_update(v_batchid, v_rec_notfound, 0);
													RAISE NOTICE 'End of Control table update - 2';
													END IF;																			
											END;
					
							END IF; 
--END;

							
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
