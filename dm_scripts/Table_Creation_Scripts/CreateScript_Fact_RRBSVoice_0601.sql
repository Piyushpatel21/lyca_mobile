CREATE TABLE IF NOT EXISTS uk_rrbs_dm.fact_rrbs_voice
(
	sk_rrbs_voice BIGINT  DEFAULT default_identity(138121, 0, '1,1'::text) ENCODE zstd
	,voice_call_cdr INTEGER   ENCODE zstd
	,network_id INTEGER   ENCODE RAW
	,call_type INTEGER   ENCODE RAW
	,call_feature INTEGER   ENCODE zstd
	,tariffplan_id INTEGER   ENCODE zstd
	,service_id INTEGER   ENCODE zstd
	,cli BIGINT   ENCODE zstd
	,dialed_number VARCHAR(50)   ENCODE zstd
	,charged_party_number BIGINT   ENCODE zstd
	,imsi BIGINT   ENCODE zstd
	,serving_node BIGINT   ENCODE zstd
	,mcc INTEGER   ENCODE zstd
	,mnc INTEGER   ENCODE zstd
	,lac INTEGER   ENCODE zstd
	,calling_cell_id INTEGER   ENCODE zstd
	,cellzone_code VARCHAR(30)   ENCODE zstd
	,cellzone_name VARCHAR(30)   ENCODE zstd
	,original_dialed_number BIGINT   ENCODE zstd
	,destination_zone INTEGER   ENCODE zstd
	,destination_area_code BIGINT   ENCODE zstd
	,destinationzone_name VARCHAR(30)   ENCODE bytedict
	,roaming_area_code VARCHAR(20)   ENCODE zstd
	,roaming_zone_name VARCHAR(30)   ENCODE zstd
	,roam_flag SMALLINT   ENCODE RAW
	,roam_area_number INTEGER   ENCODE zstd
	,granted_time INTEGER   ENCODE zstd
	,granted_money NUMERIC(18,0)   ENCODE zstd
	,call_duration INTEGER   ENCODE zstd
	,chargeable_used_time INTEGER   ENCODE zstd
	,call_date TIMESTAMP WITHOUT TIME ZONE   ENCODE ZSTD
	/* 080120-addition: adding derived columns*/
	,call_date_dt DATE ENCODE RAW
	,call_date_num INT ENCODE ZSTD
	/* end of 080120-addition*/
	,call_termination_time TIMESTAMP WITHOUT TIME ZONE   ENCODE zstd
	,initial_account_balance NUMERIC(10,4)   ENCODE zstd
	,talk_charge NUMERIC(10,4)   ENCODE zstd
	,balance NUMERIC(10,4)   ENCODE zstd
	,free_minutes_account_balance NUMERIC(20,6)   ENCODE zstd
	,account_id INTEGER   ENCODE zstd
	,dept_id INTEGER   ENCODE zstd
	,free_zone_id INTEGER   ENCODE zstd
	,trace_id VARCHAR(50)   ENCODE zstd
	,last_topup_type SMALLINT   ENCODE zstd
	,bundle_code VARCHAR(10)   ENCODE zstd
	,free_zone_expiry_date VARCHAR(50)   ENCODE zstd
	,ringing_duration VARCHAR(3)   ENCODE zstd
	,ring_indicator VARCHAR(1)   ENCODE zstd
	,national_bundle_code VARCHAR(10)   ENCODE zstd
	,national_used_minutes VARCHAR(10)   ENCODE zstd
	,national_charge VARCHAR(10)   ENCODE zstd
	,pool_number VARCHAR(20)   ENCODE zstd
	,bundle_used_seconds VARCHAR(10)   ENCODE zstd
	,bundle_call_charge VARCHAR(10)   ENCODE zstd
	,bundle_balance VARCHAR(10)   ENCODE zstd
	,bundle_plan_id VARCHAR(5)   ENCODE zstd
	,final_unit_indicator VARCHAR(1)   ENCODE zstd
	,ncall_free_units VARCHAR(10)   ENCODE zstd
	,local_roam_country_code VARCHAR(4)   ENCODE zstd
	,cdr_time_stamp TIMESTAMP WITHOUT TIME ZONE   ENCODE zstd
	,bucket_type INTEGER   ENCODE zstd
	,conversion_unit VARCHAR(50)   ENCODE zstd
	,intial_free_units INTEGER   ENCODE zstd
	,subscriber_type INTEGER   ENCODE zstd
	,call_forwarding_indicator INTEGER   ENCODE zstd
	,cdr_sequence_number INTEGER   ENCODE zstd
	,sub_acct_id INTEGER   ENCODE zstd
	,announcement_time INTEGER   ENCODE zstd
	,family_id INTEGER   ENCODE zstd
	,idp_time VARCHAR(100)   ENCODE zstd
	,cs_free_mins VARCHAR(50)   ENCODE zstd
	,free_units_detected INTEGER   ENCODE zstd
	,multileg_charging_flag INTEGER   ENCODE zstd
	,extension_record INTEGER   ENCODE zstd
	,connect_number VARCHAR(50)   ENCODE zstd
	,dialed_number1 VARCHAR(50)   ENCODE zstd
	,service_description VARCHAR(100)   ENCODE zstd
	,alternate_plan_id VARCHAR(50)   ENCODE zstd
	,serving_node_name VARCHAR(50)   ENCODE zstd
	,fixed_charge INTEGER   ENCODE zstd
	,fixed_charge_resource_impact VARCHAR(50)   ENCODE zstd
	,bundle_version_name VARCHAR(50)   ENCODE zstd
	,flh_reward_slab_id INTEGER   ENCODE zstd
	,bundle_version_id INTEGER   ENCODE zstd
	,rate_id VARCHAR(50)   ENCODE zstd
	,pricing_plan_id VARCHAR(50)   ENCODE zstd
	,pocket_id VARCHAR(50)   ENCODE zstd
	,consumed_promo_amount VARCHAR(50)   ENCODE zstd
	,pervious_call_trace_id VARCHAR(50)   ENCODE zstd
	,termination_cause INTEGER   ENCODE zstd
	,non_chargeable_unit INTEGER   ENCODE zstd
	,call_forward_msisdn VARCHAR(50)   ENCODE zstd
	,service_type VARCHAR(50)   ENCODE zstd
	,used_time_in_previous_grant INTEGER   ENCODE zstd
	,rlh_charging_indicator INTEGER   ENCODE zstd
	,surcharge_consumed INTEGER   ENCODE zstd
	,excess_used_units_received INTEGER   ENCODE zstd
	,failcause INTEGER   ENCODE zstd
	,roaming_partner_id INTEGER   ENCODE zstd
	,filename VARCHAR(100) NOT NULL  ENCODE zstd
	,batch_id INTEGER NOT NULL  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE NOT NULL  ENCODE zstd
)
DISTSTYLE KEY
 DISTKEY (imsi)
 SORTKEY (
	call_date_dt
	, network_id
	, roam_flag
	, call_type
	)
;

