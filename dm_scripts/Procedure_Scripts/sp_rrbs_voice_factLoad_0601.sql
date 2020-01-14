CREATE OR REPLACE PROCEDURE uk_rrbs_dm.sp_rrbs_voice_factLoad(p_filename "varchar")
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
						SELECT COUNT(1) INTO v_check_stage FROM uk_rrbs_stg.stg_rrbs_voice WHERE filename = v_filename ;
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

								SELECT top 1 filename into v_dummy_2 FROM uk_rrbs_dm.fact_rrbs_voice WHERE filename = v_filename ;
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
										uk_rrbs_dm.sp_rrbs_cntrlFact_insert (v_filename, v_batchid, 'RRBS-VOICE', v_check_stage )		;--(p_filename "varchar",p_batchid "int4", p_srcSys "varchar", p_stgCount "int4");
										RAISE NOTICE 'Batch-id and Filename inserted in Control table';
									END IF;
									END;																																
								
											BEGIN	
												RAISE NOTICE 'Inserting Records into fact table';
												INSERT INTO
														uk_rrbs_dm.fact_rrbs_voice
														
														(
Voice_call_cdr                 ,
Network_ID                     ,
Call_Type                      ,
call_feature                   ,
TariffPlan_ID                  ,
Service_ID                     ,
cli                            ,
Dialed_number                  ,
Charged_party_number           ,
IMSI                           ,
Serving_Node                   ,
MCC                            ,
MNC                            ,
LAC                            ,
Calling_Cell_ID                ,
CellZone_Code                  ,
CellZone_Name                  ,
Original_Dialed_number         ,
Destination_zone               ,
Destination_area_code          ,
Destinationzone_name           ,
Roaming_area_code              ,
Roaming_zone_name              ,
roam_flag                      ,
Roam_area_number               ,
Granted_Time                   ,
Granted_Money                  ,
call_duration                  ,
Chargeable_Used_Time           ,
Call_date                      ,
/* 080120-addition: adding derived columns*/
call_date_dt					,
call_date_num 					,
/* end of 080120-addition*/
Call_termination_time          ,
Initial_Account_balance        ,
Talk_charge                    ,
balance                        ,
Free_minutes_account_balance   ,
Account_Id                     ,
Dept_Id                        ,
Free_Zone_Id                   ,
Trace_Id                       ,
Last_Topup_Type                ,
Bundle_Code                    ,
Free_Zone_Expiry_Date          ,
Ringing_Duration               ,
Ring_Indicator                 ,
National_Bundle_Code           ,
National_Used_Minutes          ,
National_Charge                ,
Pool_Number                    ,
Bundle_used_seconds            ,
Bundle_Call_Charge             ,
Bundle_Balance                 ,
Bundle_Plan_ID                 ,
Final_Unit_Indicator           ,
NCALL_Free_Units               ,
Local_Roam_Country_Code        ,
CDR_Time_Stamp                 ,
Bucket_type                    ,
Conversion_Unit                ,
Intial_Free_units              ,
Subscriber_Type                ,
Call_Forwarding_Indicator      ,
CDR_Sequence_Number            ,
Sub_Acct_Id                    ,
Announcement_Time              ,
Family_id                      ,
Idp_Time                       ,
CS_free_Mins                   ,
Free_Units_Detected            ,
Multileg_Charging_Flag         ,
Extension_Record               ,
Connect_Number                 ,
Dialed_Number1                 ,
Service_Description            ,
Alternate_Plan_Id              ,
Serving_Node_Name              ,
Fixed_charge                   ,
Fixed_charge_Resource_Impact   ,
Bundle_Version_Name            ,
FLH_Reward_slab_id             ,
Bundle_version_ID              ,
Rate_ID                        ,
Pricing_Plan_ID                ,
Pocket_Id                      ,
Consumed_Promo_Amount          ,
Pervious_call_trace_Id         ,
Termination_Cause              ,
Non_Chargeable_Unit            ,
Call_Forward_MSISDN            ,
Service_Type                   ,
Used_time_in_previous_grant    ,
RLH_Charging_Indicator         ,
Surcharge_Consumed             ,
Excess_Used_units_received     ,
Failcause                      ,
Roaming_Partner_ID             ,
FileName                       ,
batch_id					   ,
CREATED_DATE																                   
														)
												SELECT
		Voice_call_cdr                                   ,
        NULLIF(Network_ID,'') ::integer                                   ,
        NULLIF(Call_Type,'') ::integer                                    ,
        NULLIF(call_feature,'') ::integer                                 ,
        TariffPlan_ID                                          ,
        Service_ID                                             ,
        nullif(cli,'') ::BIGINT                                           ,
        Dialed_number                                  ,
        Charged_party_number ::BIGINT                          ,
        NULLIF(IMSI,'') ::BIGINT                                          ,
        NULLIF(Serving_Node,'') ::BIGINT                                  ,
        NULLIF(MCC,'') ::integer                                          ,
        NULLIF(MNC,'') ::integer                                          ,
        NULLIF(LAC,'') ::integer                                          ,
        NULLIF(Calling_Cell_ID,'') ::integer                              ,
        CellZone_Code                                          ,
        CellZone_Name                                          ,
        NULLIF(Original_Dialed_number,'')::BIGINT              ,
        NULLIF(Destination_zone,'') ::integer                  ,
        NULLIF(Destination_area_code,'') ::BIGint             ,
        Destinationzone_name                                   ,
        Roaming_area_code                                      ,
        Roaming_zone_name                                      ,
        NULLIF(roam_flag,'') ::smallint                        ,
        NULLIF(Roam_area_number,'') ::integer                  ,
        NULLIF(Granted_Time,'') ::integer                      ,
        Granted_Money                                          ,
        NULLIF(call_duration,'') ::integer                     ,
        NULLIF(Chargeable_Used_Time,'') ::integer              ,
        TO_TIMESTAMP(Call_date , 'YYYYMMDDHHMISS')             ,
/* 080120-addition: adding derived columns*/
to_date(SUBSTRING(NULLIF(NULLIF(call_date, '0'), ''), 1, 8), 'yyyymmdd') AS call_date_dt	,		
to_number((SUBSTRING(NULLIF(NULLIF(call_date, '0'), ''), 1, 8)), '999999999') AS call_date_num			,		
/* end of 080120-addition*/
        TO_TIMESTAMP(Call_termination_time , 'YYYYMMDDHHMISS') ,
        Initial_Account_balance                                ,
        Talk_charge                                            ,
        balance                                                ,
        Free_minutes_account_balance                           ,
        Account_Id                                             ,
        Dept_Id                                                ,
        NULLIF(Free_Zone_Id,'') ::integer                      ,
        Trace_Id                                               ,
        NULLIF(Last_Topup_Type,'') ::smallint                  ,
        Bundle_Code                                            ,
        --to_date(NULLIF(Free_Zone_Expiry_Date,''), 'DD-MM-YYYY')           ,
		Free_Zone_Expiry_Date									,
        Ringing_Duration                                       ,
        Ring_Indicator                                         ,
        National_Bundle_Code                                   ,
        National_Used_Minutes                                  ,
        National_Charge                                        ,
        Pool_Number                                            ,
        Bundle_used_seconds                                    ,
        Bundle_Call_Charge                                     ,
        Bundle_Balance                                         ,
        Bundle_Plan_ID                                         ,
        Final_Unit_Indicator                                   ,
        NCALL_Free_Units                                       ,
        Local_Roam_Country_Code                                ,
        TO_TIMESTAMP(CDR_Time_Stamp , 'YYYYMMDDHHMISS')        ,
        NULLIF(Bucket_type,'') ::integer                       ,
        Conversion_Unit                                        ,
        NULLIF(Intial_Free_units,'') ::DECIMAL(20,4)           ,
        NULLIF(Subscriber_Type,'') ::integer                   ,
        NULLIF(Call_Forwarding_Indicator,'') ::integer         ,
        NULLIF(CDR_Sequence_Number,'') ::integer               ,
        NULLIF(Sub_Acct_Id,'') ::integer                       ,
        NULLIF(Announcement_Time,'') ::integer                 ,
        NULLIF(Family_id,'') ::integer                         ,
        Idp_Time                                               ,
        CS_free_Mins                                           ,
        NULLIF(Free_Units_Detected,'') ::DECIMAL(10,4)         ,
        NULLIF(Multileg_Charging_Flag,'') ::integer            ,
        NULLIF(Extension_Record,'') ::integer                  ,
        Connect_Number                                         ,
        Dialed_Number1                     ,
        Service_Description                                    ,
        Alternate_Plan_Id                                      ,
        Serving_Node_Name                                      ,
        NULLIF(Fixed_charge,'') ::DECIMAL(20,6)                ,
        Fixed_charge_Resource_Impact                           ,
        Bundle_Version_Name                                    ,
        NULLIF(FLH_Reward_slab_id,'') ::integer                ,
        NULLIF(Bundle_version_ID,'') ::integer                 ,
        Rate_ID                                                ,
        Pricing_Plan_ID                                        ,
        Pocket_Id                                              ,
        Consumed_Promo_Amount                                  ,
        Pervious_call_trace_Id                                 ,
        NULLIF(Termination_Cause,'') ::integer                 ,
        NULLIF(Non_Chargeable_Unit,'') ::integer               ,
        Call_Forward_MSISDN                                    ,
        Service_Type                                           ,
        NULLIF(Used_time_in_previous_grant,'') ::integer       ,
        NULLIF(RLH_Charging_Indicator,'') ::integer            ,
        NULLIF(Surcharge_Consumed,'') ::DECIMAL(20,6)          ,
        NULLIF(Excess_Used_units_received,'') ::integer        ,
        NULLIF(Failcause,'') ::integer                         ,
        NULLIF(Roaming_Partner_ID,'') ::integer                ,
        FileName                                               ,
        v_batchid											   ,
        CREATED_DATE
       
FROM
        uk_rrbs_stg.stg_rrbs_voice sruv
														
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
