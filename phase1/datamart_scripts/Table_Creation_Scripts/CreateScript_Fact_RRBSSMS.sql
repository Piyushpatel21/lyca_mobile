
CREATE TABLE IF NOT EXISTS uk_rrbs_dm.fact_rrbs_sms
(
	sk_rrbs_sms BIGINT  GENERATED BY DEFAULT AS identity(1,1) ENCODE zstd
	,cdr_types SMALLINT   ENCODE RAW
	,network_id SMALLINT   ENCODE RAW
	,call_type SMALLINT   ENCODE RAW
	,plan_id INTEGER   ENCODE zstd
	,service_id SMALLINT   ENCODE zstd
	,cli BIGINT   ENCODE zstd
	,dialed_number VARCHAR(50)   ENCODE zstd
	,imsi BIGINT   ENCODE zstd
	,serving_node BIGINT   ENCODE zstd
	,destination_area_code INTEGER   ENCODE zstd
	,destination_zone_code INTEGER   ENCODE zstd
	,destination_zone_name VARCHAR(50)   ENCODE zstd
	,roam_flag INTEGER   ENCODE RAW
	,roaming_node INTEGER   ENCODE zstd
	,roaming_area_code INTEGER   ENCODE zstd
	,roaming_zone_name VARCHAR(50)   ENCODE zstd
	,sms_feature INTEGER   ENCODE zstd
	,number_of_sms_charged INTEGER   ENCODE zstd
	,number_of_free_sms INTEGER   ENCODE zstd
	,initial_account_balance NUMERIC(10,4)   ENCODE zstd
	,msg_cost NUMERIC(10,4)   ENCODE zstd
	,balance NUMERIC(10,4)   ENCODE zstd
	,free_sms_account_balance INTEGER   ENCODE zstd
	,instance_id_session_id VARCHAR(20)   ENCODE zstd
	,msg_date TIMESTAMP WITHOUT TIME ZONE   ENCODE RAW
	
	/* 080120-addition: adding derived columns*/
	,msg_date_dt DATE ENCODE RAW
	,msg_date_num INT ENCODE ZSTD
	/* end of 080120-addition*/
	
	,account_id INTEGER   ENCODE zstd
	,dept_id INTEGER   ENCODE zstd
	,free_zone_id INTEGER   ENCODE zstd
	,last_topup_type SMALLINT   ENCODE zstd
	,bundle_code INTEGER   ENCODE zstd
	,free_zone_expiry_date DATE   ENCODE zstd
	,bundle_sms_charge NUMERIC(10,4)   ENCODE zstd
	,bundle_balance NUMERIC(10,4)   ENCODE zstd
	,bundle_plan_id INTEGER   ENCODE zstd
	,nsms_free_units INTEGER   ENCODE zstd
	,local_roam_country_code SMALLINT   ENCODE zstd
	,cdr_time_stamp TIMESTAMP WITHOUT TIME ZONE   ENCODE zstd
	,bucket_type SMALLINT   ENCODE zstd
	,initial_free_units NUMERIC(20,4)   ENCODE zstd
	,subscriber_type SMALLINT   ENCODE zstd
	,sub_acct_id BIGINT   ENCODE zstd
	,family_id VARCHAR(50)   ENCODE zstd
	,concat_message_flag SMALLINT   ENCODE zstd
	,concat_message_reference_number INTEGER   ENCODE zstd
	,concat_message_total_chunks INTEGER   ENCODE zstd
	,concat_message_current_chunk INTEGER   ENCODE zstd
	,trace_id VARCHAR(50)   ENCODE zstd
	,idp_time VARCHAR(50)   ENCODE zstd
	,extension_record SMALLINT   ENCODE zstd
	,call_indicator SMALLINT   ENCODE zstd
	,serving_node_name VARCHAR(50)   ENCODE zstd
	,bundle_version_name VARCHAR(50)   ENCODE zstd
	,flh_reward_slab_id INTEGER   ENCODE zstd
	,bundle_version_id SMALLINT   ENCODE zstd
	,rate_id INTEGER   ENCODE zstd
	,pricing_plan_id SMALLINT   ENCODE zstd
	,pocket_id BIGINT   ENCODE zstd
	,consumed_promo_amount NUMERIC(10,4)   ENCODE zstd
	,rlh_charging_indicator SMALLINT   ENCODE zstd
	,surcharge_consumed VARCHAR(50)   ENCODE zstd
	,roaming_partner_id VARCHAR(50)   ENCODE zstd
	,filename VARCHAR(100) NOT NULL  ENCODE zstd
	,batch_id INTEGER NOT NULL  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE NOT NULL  ENCODE zstd
)
DISTSTYLE KEY
 DISTKEY (imsi)
 SORTKEY (
	msg_date_dt
	, network_id
	, cdr_types
	, roam_flag
	, call_type
	)
;

