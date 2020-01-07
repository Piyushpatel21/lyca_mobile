CREATE OR REPLACE PROCEDURE uk_rrbs_dm.sp_rrbs_topup_factLoad(p_filename "varchar")
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
						SELECT COUNT(1) INTO v_check_stage_count FROM uk_rrbs_stg.stg_rrbs_topup WHERE filename = v_filename ;
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

								SELECT top 1 filename into v_dummy_2 FROM uk_rrbs_dm.fact_rrbs_topup WHERE filename = v_filename ;
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
										uk_rrbs_dm.sp_rrbs_cntrlFact_insert (v_filename, v_batchid, 'RRBS-TOPUP', v_check_stage_count )		;--(p_filename "varchar",p_batchid "int4", p_srcSys "varchar", p_stgCount "int4");
										RAISE NOTICE 'Batch-id, Filename, Stage-Count inserted in Control table';
									END IF;
									END;																																
								
									BEGIN	
									RAISE NOTICE 'Inserting Records into fact table';
												INSERT INTO
												uk_rrbs_dm.fact_rrbs_topup														
												(
													operation_code
,network_id
,recharge_type
,msisdn
,imsi
,account_pin_number
,voucher_card_id
,special_topup_amount
,transaction_id
,new_balance
,face_value
,recharge_amount
,promo_validity_date
,free_minutes
,free_voucher_onnet_minutes
,free_minutes_expiry_date
,free_sms
,free_voucher_onnet_sms
,free_sms_expiry_date
,free_offnet_minutes
,free_voucher_offnet_minutes
,free_offnet_minutes_expiry_date
,free_offnet_sms
,free_voucher_offnet_sms
,free_offnet_sms_expiry_date
,free_offnet2_minutes
,free_vouchercard_offnet2_minutes
,free_offnet2_minutes_expiry_date
,free_offnet2_sms
,free_vouchercard_offnet2_sms
,free_offnet2_sms_expiry_date
,free_offnet3_minutes
,free_vouchercard_offnet3_minutes
,free_offnet3_minutes_expiry_date
,free_offnet3_sms
,free_vouchercard_offnet3_sms
,free_offnet3_sms_expiry_date
,free_data
,free_voucher_data
,free_data_expirydate
,account_validity_date
,lyca_voucher_card_id
,planid
,topup_counter
,bundle_code
,imei
,voucher_onnet_mins_expdt
,voucher_onnet_sms_expdt
,voucher_offnet_mins_expdt1
,voucher_offnet_sms_expdt1
,voucher_offnet_mins_expdt2
,voucher_offnet_sms_expdt2
,voucher_offnet_mins_expdt3
,voucher_offnet_sms_expdt3
,voucher_free_dataexp
,retailer_msisdn
,staff_msisdn
,wlm_topupmode
,face_value_1
,promo_validity_days
,oldplanid
,onnet_mt_mins
,voucher_onnet_mt_mins
,onnet_mt_sms
,voucher_onnet_mt_sms
,onnet_mt_exp_dt
,voucher_onnet_mt_expiry_date
,offnet_mt_mins
,voucher_offnet_mt_mins
,offnet_mt_sms
,voucher_offnet_mt_sms
,offnet_mt_exp_dt
,voucher_offnet_mt_expiry_date
,bundle_name
,old_account_balance
,parent_msisdn
,rrbs_transaction_id
,override_bundle
,vouchernetworkid
,accid
,deptid
,bundle_catery
,plan_validity_days
,account_id
,sim_number
,cdr_time_stamp
,reservation
,discount_promo_code
,discount_promo_amount
,primary_bundle_code
,family_account_id
,tax
,number_of_installments
,oba_bundle_flag
,oba_due_amount
,parent_bundle_code
,bundle_unit_type
,contract_start_date
,contract_end_date
,flh_flag
,exibs_retailer_transaction_id
,operation_flag
,vat_amount
,special_discount_code
,special_discount_amount
,number_of_installments_discounts_applied
,actual_bundle_cost
,reservation_reference_transaction_id
,bundle_expiry_type
,bundle_balance
,muv_indicator
,bundle_group_type
,sequence_number
,amount_refunded
,forcible_cancellation
,payment_mode_indicator
,amount
,version_id
,refund_entity_type
,refund_provided_to
,topup_validity_days
,reserve_amount
,preloaded_bundle_flag
,staff_indicator
,child_addition_discount
,payg_family_indicator
,payg_prorate_flag
,payg_child_index
,parent_msisdn1
,payg_member_count
,direct_addon_flag
,action_flag
,"timestamp"
,sim_with_bundle_code
,taxation_details
,reserved
,bundle_purchase_date
,bundle_start_date
,renewal_payment_mode
,iot_bundle_flag
,veekbundleflag
,validity_overridden
,link_type
,overwrite_bundle_cost_flag
,order_id
,pocket_id
,da_track
,da_principal_amount
,is_loan_request
,migrated_ortel_subscriber_flag
,reward_purchased
,feebuffer
,tax_buffer
,paymentgateway
,zipcode
,professional_channel_name
,topupcriteriaflag
,nus_id
,nus_applied
,slotid
,reserved_bundle_cost
,filename
,batch_id
,created_date		                   
														)
												SELECT
		        NULLIF(operation_code,''):: smallint
,NULLIF(network_id,''):: smallint
,NULLIF(recharge_type,''):: smallint
,NULLIF(msisdn,''):: bigint
,NULLIF(imsi,''):: bigint
,NULLIF(account_pin_number,''):: bigint
,NULLIF(voucher_card_id,''):: int
,special_topup_amount::decimal(10,4)
,transaction_id 
,new_balance::decimal(10,4) 
,face_value ::decimal(10,4)
,rechage_amount ::decimal(10,4)
,to_date(nullif(promo_validity_date,''),'dd-mm-yyyy') 
,NULLIF(free_minutes,''):: int
,NULLIF(free_voucher_onnet_minutes,''):: int
,to_date(nullif(free_minutes_expiry_date,''),'dd-mm-yyyy') 
,NULLIF(free_sms,''):: int
,NULLIF(free_voucher_onnet_sms,''):: int
,to_date(nullif(free_sms_expiry_date,''),'dd-mm-yyyy') 
,NULLIF(free_offnet_minutes,''):: int
,NULLIF(free_voucher_offnet_minutes,''):: int
,to_date(nullif(free_offnet_minutes_expiry_date,''),'dd-mm-yyyy') 
,NULLIF(free_offnet_sms,''):: int
,NULLIF(free_voucher_offnet_sms,''):: int
,to_date(nullif(free_offnet_sms_expiry_date,''),'dd-mm-yyyy') 
,NULLIF(free_offnet2_minutes,''):: int
,NULLIF(free_vouchercard_offnet2_minutes,''):: int
,to_date(nullif(free_offnet2_minutes_expiry_date,''),'dd-mm-yyyy') 
,NULLIF(free_offnet2_sms,''):: int
,NULLIF(free_vouchercard_offnet2_sms,''):: int
,to_date(nullif(free_offnet2_sms_expiry_date,''),'dd-mm-yyyy') 
,NULLIF(free_offnet3_minutes,''):: int
,NULLIF(free_vouchercard_offnet3_minutes,''):: int
,to_date(nullif(free_offnet3_minutes_expiry_date,''),'dd-mm-yyyy') 
,NULLIF(free_offnet3_sms,''):: int
,NULLIF(free_vouchercard_offnet3_sms,''):: int
,to_date(nullif(free_offnet3_sms_expiry_date,''),'dd-mm-yyyy') 
,NULLIF(free_data,''):: bigint
,NULLIF(free_voucher_data,''):: bigint
,to_date(nullif(free_data_expirydate,''),'dd-mm-yyyy') 
,to_date(nullif(account_validity_date,''),'dd-mm-yyyy') 
,lyca_voucher_card_id 
,NULLIF(planid,''):: smallint
,NULLIF(topup_counter,''):: smallint
,NULLIF(bundle_code,''):: INT
,imei 
,to_date(nullif(voucher_onnet_mins_expdt,''),'dd-mm-yyyy') 
,to_date(nullif(voucher_onnet_sms_expdt,''),'dd-mm-yyyy') 
,to_date(nullif(voucher_offnet_mins_expdt1,''),'dd-mm-yyyy') 
,to_date(nullif(voucher_offnet_sms_expdt1,''),'dd-mm-yyyy') 
,to_date(nullif(voucher_offnet_mins_expdt2,''),'dd-mm-yyyy') 
,to_date(nullif(voucher_offnet_sms_expdt2,''),'dd-mm-yyyy') 
,to_date(nullif(voucher_offnet_mins_expdt3,''),'dd-mm-yyyy') 
,to_date(nullif(voucher_offnet_sms_expdt3,''),'dd-mm-yyyy') 
,to_date(nullif(voucher_free_dataexp,''),'dd-mm-yyyy') 
,retailer_msisdn 
,staff_msisdn 
,wlm_topupmode 
,NULLIF(face_value_1,''):: decimal(10,4)
,NULLIF(promo_validity_days,''):: int
,NULLIF(oldplanid,''):: int
,NULLIF(onnet_mt_mins,''):: int
,NULLIF(voucher_onnet_mt_mins,''):: int
,NULLIF(onnet_mt_sms,''):: int
,NULLIF(voucher_onnet_mt_sms,''):: int
,to_date(nullif(onnet_mt_exp_dt,''),'dd-mm-yyyy') 
,to_date(nullif(voucher_onnet_mt_expiry_date,''),'dd-mm-yyyy') 
,NULLIF(offnet_mt_mins,''):: int
,NULLIF(voucher_offnet_mt_mins,''):: int
,NULLIF(offnet_mt_sms,''):: int
,NULLIF(voucher_offnet_mt_sms,''):: int
,to_date(nullif(offnet_mt_exp_dt,''),'dd-mm-yyyy') 
,to_date(nullif(voucher_offnet_mt_expiry_date,''),'dd-mm-yyyy') 
,bundle_name 
,NULLIF(old_account_balance,''):: decimal(10,4)
,parent_msisdn 
,rrbs_transaction_id 
,NULLIF(override_bundle,''):: int
,NULLIF(vouchernetworkid,''):: smallint
,accid 
,deptid 
,NULLIF(bundle_catery,''):: smallint
,NULLIF(plan_validity_days,''):: smallint
,NULLIF(account_id,''):: int
,NULLIF(sim_number,''):: bigint
,to_timestamp(nullif(cdr_time_stamp,0),'yyyymmddhhmiss') 
,NULLIF(reservation,''):: smallint
,discount_promo_code 
,NULLIF(discount_promo_amount,''):: decimal(10,4)
,NULLIF(primary_bundle_code,''):: int
,family_account_id 
,NULLIF(tax,''):: decimal(10,4)
,NULLIF(number_of_installments,''):: int
,NULLIF(oba_bundle_flag,''):: smallint
,NULLIF(oba_due_amount,''):: decimal(10,4)
,NULLIF(parent_bundle_code,''):: int
,NULLIF(bundle_unit_type,''):: smallint
,to_date(nullif(contract_start_date,''),'dd-mm-yyyy') 
,to_date(nullif(contract_end_date,''),'dd-mm-yyyy') 
,NULLIF(flh_flag,''):: smallint
,exibs_retailer_transaction_id 
,NULLIF(operation_flag,''):: smallint
,NULLIF(vat_amount,''):: decimal(10,4)
,special_discount_code 
,NULLIF(special_discount_amount,''):: decimal(10,4)
,number_of_installments_discounts_applied 
,NULLIF(actual_bundle_cost,''):: decimal(10,4)
,reservation_reference_transaction_id 
,NULLIF(bundle_expiry_type,''):: smallint
,NULLIF(bundle_balance,''):: decimal(10,4)
,NULLIF(muv_indicator,''):: smallint
,NULLIF(bundle_group_type,''):: smallint
,NULLIF(sequence_number,''):: smallint
,NULLIF(amount_refunded,''):: decimal(10,4)
,forcible_cancellation 
,payment_mode_indicator 
,NULLIF(amount,''):: decimal(20,4)
,NULLIF(version_id,''):: smallint
,NULLIF(refund_entity_type,''):: smallint
,refund_provided_to 
,NULLIF(topup_validity_days,''):: int
,NULLIF(reserve_amount,''):: decimal(10,4)
,NULLIF(preloaded_bundle_flag,''):: smallint
,NULLIF(staff_indicator,''):: smallint
,NULLIF(child_addition_discount,''):: decimal(10,4)
,NULLIF(payg_family_indicator,''):: smallint
,NULLIF(payg_prorate_flag,''):: smallint
,NULLIF(payg_child_index,''):: smallint
,parent_msisdn1 
,NULLIF(payg_member_count,''):: int
,NULLIF(direct_addon_flag,''):: smallint
,NULLIF(action_flag,''):: smallint
,"timestamp"
,NULLIF(sim_with_bundle_code,''):: int
,taxation_details 
,reserved 
,to_timestamp(nullif(nullif(bundle_purchase_date,'0'),''),'yyyymmddhhmiss') 
,to_timestamp(nullif(nullif(bundle_start_date,'0'),''),'yyyymmddhhmiss') 
,NULLIF(renewal_payment_mode,''):: smallint
,NULLIF(iot_bundle_flag,''):: smallint
,NULLIF(veekbundleflag,''):: smallint
,NULLIF(validity_overridden,''):: smallint
,NULLIF(link_type,''):: smallint
,NULLIF(overwrite_bundle_cost_flag,''):: smallint
,order_id 
,pocket_id 
,NULLIF(da_track,''):: decimal(10,4)
,NULLIF(da_principal_amount,''):: decimal(10,4)
,NULLIF(is_loan_request,''):: smallint
,NULLIF(migrated_ortel_subscriber_flag,''):: smallint
,NULLIF(reward_purchased,''):: smallint
,feebuffer 
,tax_buffer 
,paymentgateway 
,zipcode 
,professional_channel_name 
,NULLIF(topupcriteriaflag,''):: smallint
,nus_id 
,NULLIF(nus_applied,''):: smallint
,slotid 
,reserved_bundle_cost 
,filename 
,v_batchId
,created_date
												FROM
        										uk_rrbs_stg.stg_rrbs_topup srut														
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
														CALL uk_rrbs_dm.sp_rrbs_cntrlFact_update (v_batchid, v_complete_status, fact_insert_count);
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
