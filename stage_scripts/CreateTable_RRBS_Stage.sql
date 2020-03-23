---#### 1. RRBS Voice table :"stg_rrbs_voice"----------------------------------

CREATE TABLE IF NOT EXISTS uk_rrbs_stg.stg_rrbs_voice
(
	voice_call_cdr INTEGER   ENCODE zstd
	,network_id VARCHAR(10)   ENCODE zstd
	,call_type VARCHAR(10)   ENCODE zstd
	,call_feature VARCHAR(10)   ENCODE zstd
	,tariffplan_id INTEGER   ENCODE zstd
	,service_id INTEGER   ENCODE zstd
	,cli VARCHAR(20)   ENCODE zstd
	,dialed_number VARCHAR(50)   ENCODE zstd
	,charged_party_number VARCHAR(20)   ENCODE zstd
	,imsi VARCHAR(50)   ENCODE zstd
	,serving_node VARCHAR(30)   ENCODE zstd
	,mcc VARCHAR(50)   ENCODE zstd
	,mnc VARCHAR(50)   ENCODE zstd
	,lac VARCHAR(50)   ENCODE zstd
	,calling_cell_id VARCHAR(30)   ENCODE zstd
	,cellzone_code VARCHAR(30)   ENCODE zstd
	,cellzone_name VARCHAR(30)   ENCODE zstd
	,original_dialed_number VARCHAR(30)   ENCODE zstd
	,destination_zone VARCHAR(30)   ENCODE zstd
	,destination_area_code VARCHAR(30)   ENCODE zstd
	,destinationzone_name VARCHAR(30)   ENCODE zstd
	,roaming_area_code VARCHAR(20)   ENCODE zstd
	,roaming_zone_name VARCHAR(30)   ENCODE zstd
	,roam_flag VARCHAR(10)   ENCODE zstd
	,roam_area_number VARCHAR(10)   ENCODE zstd
	,granted_time VARCHAR(10)   ENCODE zstd
	,granted_money NUMERIC(10,4)   ENCODE zstd
	,call_duration VARCHAR(10)   ENCODE zstd
	,chargeable_used_time VARCHAR(30)   ENCODE zstd
	,call_date VARCHAR(30)   ENCODE zstd
	,call_termination_time VARCHAR(30)   ENCODE zstd
	,initial_account_balance NUMERIC(10,4)   ENCODE zstd
	,talk_charge NUMERIC(10,4)   ENCODE zstd
	,balance NUMERIC(10,4)   ENCODE zstd
	,free_minutes_account_balance NUMERIC(20,6)   ENCODE zstd
	,account_id INTEGER   ENCODE zstd
	,dept_id INTEGER   ENCODE zstd
	,free_zone_id VARCHAR(50)   ENCODE zstd
	,trace_id VARCHAR(50)   ENCODE zstd
	,last_topup_type VARCHAR(1)   ENCODE zstd
	,bundle_code VARCHAR(10)   ENCODE zstd
	,free_zone_expiry_date VARCHAR(15)   ENCODE zstd
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
	,cdr_time_stamp VARCHAR(50)   ENCODE zstd
	,bucket_type VARCHAR(50)   ENCODE zstd
	,conversion_unit VARCHAR(50)   ENCODE zstd
	,intial_free_units VARCHAR(50)   ENCODE zstd
	,subscriber_type VARCHAR(50)   ENCODE zstd
	,call_forwarding_indicator VARCHAR(50)   ENCODE zstd
	,cdr_sequence_number VARCHAR(50)   ENCODE zstd
	,sub_acct_id VARCHAR(50)   ENCODE zstd
	,announcement_time VARCHAR(50)   ENCODE zstd
	,family_id VARCHAR(50)   ENCODE zstd
	,idp_time VARCHAR(100)   ENCODE zstd
	,cs_free_mins VARCHAR(50)   ENCODE zstd
	,free_units_detected VARCHAR(50)   ENCODE zstd
	,multileg_charging_flag VARCHAR(50)   ENCODE zstd
	,extension_record VARCHAR(50)   ENCODE zstd
	,connect_number VARCHAR(50)   ENCODE zstd
	,dialed_number1 VARCHAR(50)   ENCODE zstd
	,service_description VARCHAR(100)   ENCODE zstd
	,alternate_plan_id VARCHAR(50)   ENCODE zstd
	,serving_node_name VARCHAR(50)   ENCODE zstd
	,fixed_charge VARCHAR(50)   ENCODE zstd
	,fixed_charge_resource_impact VARCHAR(50)   ENCODE zstd
	,bundle_version_name VARCHAR(50)   ENCODE zstd
	,flh_reward_slab_id VARCHAR(50)   ENCODE zstd
	,bundle_version_id VARCHAR(50)   ENCODE zstd
	,rate_id VARCHAR(50)   ENCODE zstd
	,pricing_plan_id VARCHAR(50)   ENCODE zstd
	,pocket_id VARCHAR(50)   ENCODE zstd
	,consumed_promo_amount VARCHAR(50)   ENCODE zstd
	,pervious_call_trace_id VARCHAR(50)   ENCODE zstd
	,termination_cause VARCHAR(50)   ENCODE zstd
	,non_chargeable_unit VARCHAR(50)   ENCODE zstd
	,call_forward_msisdn VARCHAR(50)   ENCODE zstd
	,service_type VARCHAR(50)   ENCODE zstd
	,used_time_in_previous_grant VARCHAR(50)   ENCODE zstd
	,rlh_charging_indicator VARCHAR(50)   ENCODE zstd
	,surcharge_consumed VARCHAR(50)   ENCODE zstd
	,excess_used_units_received VARCHAR(50)   ENCODE zstd
	,failcause VARCHAR(50)   ENCODE zstd
	,roaming_partner_id VARCHAR(50)   ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE zstd
	,filename VARCHAR(50) NOT NULL DEFAULT 'nofilename'::character varying ENCODE RAW
)
DISTSTYLE EVEN
 SORTKEY (
	filename
	)
;

---#### 2. RRBS GPRS table :"stg_rrbs_gprs"----------------------------------

CREATE TABLE IF NOT EXISTS uk_rrbs_stg.stg_rrbs_gprs
(
	cdr_type INTEGER   ENCODE zstd
	,network_id INTEGER   ENCODE zstd
	,data_feature INTEGER   ENCODE zstd
	,tariffplan_id INTEGER   ENCODE zstd
	,service_id INTEGER   ENCODE zstd
	,msisdn VARCHAR(20)   ENCODE zstd
	,apn VARCHAR(100)   ENCODE zstd
	,pdp_address VARCHAR(50)   ENCODE zstd
	,cellid VARCHAR(50)   ENCODE zstd
	,imei VARCHAR(50)   ENCODE zstd
	,imsi VARCHAR(20)   ENCODE zstd
	,serving_node VARCHAR(30)   ENCODE bytedict
	,ggsn_address VARCHAR(50)   ENCODE zstd
	,roaming_zone_name VARCHAR(30)   ENCODE zstd
	,roam_flag INTEGER   ENCODE zstd
	,granted_bytes_cumulative BIGINT   ENCODE zstd
	,granted_money NUMERIC(20,6)   ENCODE zstd
	,total_used_bytes BIGINT   ENCODE zstd
	,chargeable_used_bytes BIGINT   ENCODE zstd
	,uploaded_bytes INTEGER   ENCODE zstd
	,downloaded_bytes INTEGER   ENCODE zstd
	,data_connection_time VARCHAR(20)   ENCODE zstd
	,data_termination_time VARCHAR(20)   ENCODE zstd
	,time_duration VARCHAR(20)   ENCODE zstd
	,initial_account_balance NUMERIC(20,6)   ENCODE zstd
	,data_charge NUMERIC(20,6)   ENCODE zstd
	,final_account_balance NUMERIC(20,6)   ENCODE zstd
	,free_bytes VARCHAR(50)   ENCODE zstd
	,sessionid VARCHAR(50)   ENCODE zstd
	,last_topup_type VARCHAR(50)   ENCODE zstd
	,free_data_expiry_date VARCHAR(10)   ENCODE bytedict
	,charge_indicator VARCHAR(50)   ENCODE zstd
	,final_unit_indicator VARCHAR(50)   ENCODE zstd
	,bundle_code VARCHAR(10)   ENCODE bytedict
	,accid VARCHAR(11)   ENCODE lzo
	,deptid VARCHAR(15)   ENCODE lzo
	,bundle_used_data VARCHAR(10)   ENCODE zstd
	,bundle_data_charge VARCHAR(10)   ENCODE zstd
	,bundle_balance VARCHAR(10)   ENCODE zstd
	,bundle_plan_id VARCHAR(5)   ENCODE zstd
	,local_roam_country_code VARCHAR(4)   ENCODE zstd
	,sdfid VARCHAR(100)   ENCODE zstd
	,rategroupid VARCHAR(4)   ENCODE zstd
	,serviceid VARCHAR(4)   ENCODE zstd
	,cdr_time_stamp VARCHAR(20)   ENCODE zstd
	,intial_free_units VARCHAR(50)   ENCODE zstd
	,subscriber_type VARCHAR(50)   ENCODE zstd
	,cdr_sequence_number VARCHAR(50)   ENCODE zstd
	,sub_acct_id VARCHAR(50)   ENCODE zstd
	,ggsn_time VARCHAR(100)   ENCODE zstd
	,tariff_plan_change_id VARCHAR(50)   ENCODE zstd
	,used_units_before_tariff_plan_change VARCHAR(50)   ENCODE zstd
	,family_account_id VARCHAR(50)   ENCODE zstd
	,bundle_version_name VARCHAR(50)   ENCODE zstd
	,bundle_versionid VARCHAR(50)   ENCODE zstd
	,reward_slab_id VARCHAR(50)   ENCODE zstd
	,pricing_plan_id VARCHAR(50)   ENCODE zstd
	,rate_id VARCHAR(50)   ENCODE zstd
	,pocket_id VARCHAR(50)   ENCODE zstd
	,consumed_promo_amount VARCHAR(50)   ENCODE zstd
	,consumed_surcharge VARCHAR(50)   ENCODE zstd
	,rlh_charging_indicator VARCHAR(50)   ENCODE zstd
	,granted_bytes_current VARCHAR(50)   ENCODE zstd
	,roaming_partner_id VARCHAR(50)   ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE zstd
	,filename VARCHAR(50) NOT NULL DEFAULT 'nofilename'::character varying ENCODE RAW
)
DISTSTYLE EVEN
 SORTKEY (
	filename
	)
;

---#### 3. RRBS SMS table :"stg_rrbs_sms"------------------------------------

CREATE TABLE IF NOT EXISTS uk_rrbs_stg.stg_rrbs_sms
(
	cdr_types VARCHAR(40)   ENCODE zstd
	,network_id VARCHAR(10)   ENCODE zstd
	,call_type VARCHAR(5)   ENCODE zstd
	,plan_id INTEGER   ENCODE zstd
	,service_id INTEGER   ENCODE zstd
	,cli VARCHAR(20)   ENCODE zstd
	,dialed_number VARCHAR(50)   ENCODE zstd
	,imsi VARCHAR(30)   ENCODE zstd
	,serving_node VARCHAR(30)   ENCODE zstd
	,destination_area_code VARCHAR(50)   ENCODE zstd
	,destination_zone_code VARCHAR(50)   ENCODE zstd
	,destination_zone_name VARCHAR(50)   ENCODE zstd
	,roam_flag VARCHAR(50)   ENCODE zstd
	,roaming_node VARCHAR(50)   ENCODE zstd
	,roaming_area_code VARCHAR(50)   ENCODE zstd
	,roaming_zone_name VARCHAR(50)   ENCODE zstd
	,sms_feature VARCHAR(50)   ENCODE zstd
	,number_of_sms_charged VARCHAR(10)   ENCODE zstd
	,number_of_free_sms VARCHAR(20)   ENCODE zstd
	,initial_account_balance NUMERIC(10,4)   ENCODE zstd
	,msg_cost NUMERIC(10,4)   ENCODE zstd
	,balance NUMERIC(10,4)   ENCODE zstd
	,free_sms_account_balance VARCHAR(100)   ENCODE zstd
	,instance_id_session_id VARCHAR(20)   ENCODE zstd
	,msg_date VARCHAR(21) NOT NULL  ENCODE zstd
	,account_id INTEGER   ENCODE lzo
	,dept_id INTEGER   ENCODE lzo
	,free_zone_id VARCHAR(10)   ENCODE zstd
	,last_topup_type VARCHAR(1)   ENCODE zstd
	,bundle_code VARCHAR(6)   ENCODE bytedict
	,free_zone_expiry_date VARCHAR(10)   ENCODE bytedict
	,bundle_sms_charge VARCHAR(10)   ENCODE zstd
	,bundle_balance VARCHAR(10)   ENCODE zstd
	,bundle_plan_id VARCHAR(5)   ENCODE zstd
	,nsms_free_units VARCHAR(10)   ENCODE zstd
	,local_roam_country_code VARCHAR(4)   ENCODE zstd
	,cdr_time_stamp VARCHAR(21)   ENCODE zstd
	,bucket_type VARCHAR(50)   ENCODE zstd
	,intial_free_units VARCHAR(50)   ENCODE zstd
	,subscriber_type VARCHAR(50)   ENCODE zstd
	,sub_acct_id VARCHAR(50)   ENCODE zstd
	,family_id VARCHAR(50)   ENCODE lzo
	,concat_message_flag VARCHAR(50)   ENCODE zstd
	,concat_message_reference_number VARCHAR(50)   ENCODE zstd
	,concat_message_total_chunks VARCHAR(50)   ENCODE zstd
	,concat_message_current_chunk VARCHAR(50)   ENCODE zstd
	,trace_id VARCHAR(50)   ENCODE zstd
	,idp_time VARCHAR(50)   ENCODE zstd
	,extension_record VARCHAR(50)   ENCODE zstd
	,call_indicator VARCHAR(50)   ENCODE zstd
	,serving_node_name VARCHAR(50)   ENCODE zstd
	,bundle_version_name VARCHAR(50)   ENCODE zstd
	,flh_reward_slab_id VARCHAR(50)   ENCODE zstd
	,bundle_version_id VARCHAR(50)   ENCODE zstd
	,rate_id VARCHAR(50)   ENCODE zstd
	,pricing_plan_id VARCHAR(50)   ENCODE zstd
	,pocket_id VARCHAR(50)   ENCODE zstd
	,consumed_promo_amount VARCHAR(50)   ENCODE zstd
	,rlh_charging_indicator VARCHAR(50)   ENCODE zstd
	,surcharge_consumed VARCHAR(50)   ENCODE zstd
	,roaming_partner_id VARCHAR(50)   ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE zstd
	,filename VARCHAR(50) NOT NULL DEFAULT 'nofilename'::character varying ENCODE RAW
)
DISTSTYLE EVEN
 SORTKEY (
	filename
	)
;


---#### 4. RRBS TOPUP table :"stg_rrbs_topup"--------------------------------


CREATE TABLE IF NOT EXISTS uk_rrbs_stg.stg_rrbs_topup
(
	operation_code VARCHAR(10)   ENCODE zstd
	,network_id VARCHAR(50)   ENCODE zstd
	,recharge_type VARCHAR(10) NOT NULL  ENCODE zstd
	,msisdn VARCHAR(50)   ENCODE zstd
	,imsi VARCHAR(30)   ENCODE zstd
	,account_pin_number VARCHAR(50)   ENCODE zstd
	,voucher_card_id VARCHAR(50)   ENCODE zstd
	,special_topup_amount NUMERIC(10,4)   ENCODE zstd
	,transaction_id VARCHAR(50)   ENCODE zstd
	,new_balance NUMERIC(10,4)   ENCODE zstd
	,face_value NUMERIC(10,4)   ENCODE zstd
	,rechage_amount NUMERIC(10,4)   ENCODE zstd
	,promo_validity_date VARCHAR(50)   ENCODE zstd
	,free_minutes VARCHAR(20)   ENCODE zstd
	,free_voucher_onnet_minutes VARCHAR(20)   ENCODE zstd
	,free_minutes_expiry_date VARCHAR(20)   ENCODE zstd
	,free_sms VARCHAR(30)   ENCODE zstd
	,free_voucher_onnet_sms VARCHAR(20)   ENCODE zstd
	,free_sms_expiry_date VARCHAR(20)   ENCODE zstd
	,free_offnet_minutes VARCHAR(20)   ENCODE zstd
	,free_voucher_offnet_minutes VARCHAR(20)   ENCODE zstd
	,free_offnet_minutes_expiry_date VARCHAR(20)   ENCODE zstd
	,free_offnet_sms VARCHAR(20)   ENCODE zstd
	,free_voucher_offnet_sms VARCHAR(20)   ENCODE zstd
	,free_offnet_sms_expiry_date VARCHAR(20)   ENCODE zstd
	,free_offnet2_minutes VARCHAR(20)   ENCODE bytedict
	,free_vouchercard_offnet2_minutes VARCHAR(20)   ENCODE zstd
	,free_offnet2_minutes_expiry_date VARCHAR(20)   ENCODE zstd
	,free_offnet2_sms VARCHAR(20)   ENCODE zstd
	,free_vouchercard_offnet2_sms VARCHAR(20)   ENCODE zstd
	,free_offnet2_sms_expiry_date VARCHAR(20)   ENCODE zstd
	,free_offnet3_minutes VARCHAR(20)   ENCODE zstd
	,free_vouchercard_offnet3_minutes VARCHAR(20)   ENCODE zstd
	,free_offnet3_minutes_expiry_date VARCHAR(20)   ENCODE zstd
	,free_offnet3_sms VARCHAR(20)   ENCODE zstd
	,free_vouchercard_offnet3_sms VARCHAR(20)   ENCODE zstd
	,free_offnet3_sms_expiry_date VARCHAR(20)   ENCODE lzo
	,free_data VARCHAR(20)   ENCODE zstd
	,free_voucher_data VARCHAR(20)   ENCODE zstd
	,free_data_expirydate VARCHAR(20)   ENCODE zstd
	,account_validity_date VARCHAR(30)   ENCODE zstd
	,lyca_voucher_card_id VARCHAR(50)   ENCODE zstd
	,planid VARCHAR(20)   ENCODE zstd
	,topup_counter VARCHAR(20)   ENCODE zstd
	,bundle_code VARCHAR(6)   ENCODE zstd
	,imei VARCHAR(21)   ENCODE zstd
	,voucher_onnet_mins_expdt VARCHAR(10)   ENCODE zstd
	,voucher_onnet_sms_expdt VARCHAR(10)   ENCODE zstd
	,voucher_offnet_mins_expdt1 VARCHAR(10)   ENCODE zstd
	,voucher_offnet_sms_expdt1 VARCHAR(10)   ENCODE zstd
	,voucher_offnet_mins_expdt2 VARCHAR(10)   ENCODE zstd
	,voucher_offnet_sms_expdt2 VARCHAR(10)   ENCODE zstd
	,voucher_offnet_mins_expdt3 VARCHAR(10)   ENCODE zstd
	,voucher_offnet_sms_expdt3 VARCHAR(10)   ENCODE lzo
	,voucher_free_dataexp VARCHAR(10)   ENCODE zstd
	,retailer_msisdn VARCHAR(21)   ENCODE zstd
	,staff_msisdn VARCHAR(21)   ENCODE lzo
	,wlm_topupmode VARCHAR(50)   ENCODE zstd
	,face_value_1 VARCHAR(10)   ENCODE zstd
	,promo_validity_days VARCHAR(3)   ENCODE zstd
	,oldplanid VARCHAR(5)   ENCODE zstd
	,onnet_mt_mins VARCHAR(8)   ENCODE zstd
	,voucher_onnet_mt_mins VARCHAR(8)   ENCODE zstd
	,onnet_mt_sms VARCHAR(8)   ENCODE zstd
	,voucher_onnet_mt_sms VARCHAR(8)   ENCODE zstd
	,onnet_mt_exp_dt VARCHAR(10)   ENCODE lzo
	,voucher_onnet_mt_expiry_date VARCHAR(10)   ENCODE lzo
	,offnet_mt_mins VARCHAR(8)   ENCODE zstd
	,voucher_offnet_mt_mins VARCHAR(8)   ENCODE zstd
	,offnet_mt_sms VARCHAR(8)   ENCODE zstd
	,voucher_offnet_mt_sms VARCHAR(8)   ENCODE zstd
	,offnet_mt_exp_dt VARCHAR(10)   ENCODE lzo
	,voucher_offnet_mt_expiry_date VARCHAR(10)   ENCODE lzo
	,bundle_name VARCHAR(100)   ENCODE zstd
	,old_account_balance VARCHAR(10)   ENCODE zstd
	,parent_msisdn VARCHAR(21)   ENCODE zstd
	,rrbs_transaction_id VARCHAR(50)   ENCODE zstd
	,override_bundle VARCHAR(50)   ENCODE zstd
	,vouchernetworkid VARCHAR(3)   ENCODE zstd
	,accid VARCHAR(10)   ENCODE lzo
	,deptid VARCHAR(10)   ENCODE zstd
	,bundle_catery VARCHAR(50)   ENCODE zstd
	,plan_validity_days VARCHAR(50)   ENCODE zstd
	,account_id VARCHAR(50)   ENCODE zstd
	,sim_number VARCHAR(50)   ENCODE zstd
	,cdr_time_stamp VARCHAR(20)   ENCODE zstd
	,reservation VARCHAR(50)   ENCODE zstd
	,discount_promo_code VARCHAR(50)   ENCODE zstd
	,discount_promo_amount VARCHAR(50)   ENCODE zstd
	,primary_bundle_code VARCHAR(50)   ENCODE zstd
	,family_account_id VARCHAR(50)   ENCODE zstd
	,tax VARCHAR(50)   ENCODE zstd
	,number_of_installments VARCHAR(100)   ENCODE zstd
	,oba_bundle_flag VARCHAR(50)   ENCODE zstd
	,oba_due_amount VARCHAR(50)   ENCODE zstd
	,parent_bundle_code VARCHAR(50)   ENCODE zstd
	,bundle_unit_type VARCHAR(50)   ENCODE zstd
	,contract_start_date VARCHAR(50)   ENCODE zstd
	,contract_end_date VARCHAR(50)   ENCODE zstd
	,flh_flag VARCHAR(50)   ENCODE zstd
	,exibs_retailer_transaction_id VARCHAR(50)   ENCODE lzo
	,operation_flag VARCHAR(50)   ENCODE zstd
	,vat_amount VARCHAR(50)   ENCODE zstd
	,special_discount_code VARCHAR(50)   ENCODE lzo
	,special_discount_amount VARCHAR(50)   ENCODE zstd
	,number_of_installments_discounts_applied VARCHAR(50)   ENCODE lzo
	,actual_bundle_cost VARCHAR(50)   ENCODE zstd
	,reservation_reference_transaction_id VARCHAR(50)   ENCODE zstd
	,bundle_expiry_type VARCHAR(50)   ENCODE zstd
	,bundle_balance VARCHAR(50)   ENCODE zstd
	,muv_indicator VARCHAR(50)   ENCODE zstd
	,bundle_group_type VARCHAR(50)   ENCODE zstd
	,sequence_number VARCHAR(50)   ENCODE zstd
	,amount_refunded VARCHAR(50)   ENCODE zstd
	,forcible_cancellation VARCHAR(50)   ENCODE zstd
	,payment_mode_indicator VARCHAR(50)   ENCODE zstd
	,amount VARCHAR(50)   ENCODE zstd
	,version_id VARCHAR(50)   ENCODE zstd
	,refund_entity_type VARCHAR(50)   ENCODE zstd
	,refund_provided_to VARCHAR(50)   ENCODE zstd
	,topup_validity_days VARCHAR(50)   ENCODE zstd
	,reserve_amount VARCHAR(50)   ENCODE zstd
	,preloaded_bundle_flag VARCHAR(50)   ENCODE zstd
	,staff_indicator VARCHAR(50)   ENCODE zstd
	,child_addition_discount VARCHAR(50)   ENCODE zstd
	,payg_family_indicator VARCHAR(50)   ENCODE zstd
	,payg_prorate_flag VARCHAR(50)   ENCODE zstd
	,payg_child_index VARCHAR(50)   ENCODE zstd
	,parent_msisdn1 VARCHAR(50)   ENCODE zstd
	,payg_member_count VARCHAR(50)   ENCODE zstd
	,direct_addon_flag VARCHAR(50)   ENCODE zstd
	,action_flag VARCHAR(50)   ENCODE zstd
	,"timestamp" VARCHAR(50)   ENCODE zstd
	,sim_with_bundle_code VARCHAR(50)   ENCODE zstd
	,taxation_details VARCHAR(50)   ENCODE lzo
	,reserved VARCHAR(50)   ENCODE zstd
	,bundle_purchase_date VARCHAR(50)   ENCODE zstd
	,bundle_start_date VARCHAR(50)   ENCODE zstd
	,renewal_payment_mode VARCHAR(50)   ENCODE zstd
	,iot_bundle_flag VARCHAR(50)   ENCODE zstd
	,veekbundleflag VARCHAR(50)   ENCODE zstd
	,validity_overridden VARCHAR(50)   ENCODE zstd
	,link_type VARCHAR(50)   ENCODE zstd
	,overwrite_bundle_cost_flag VARCHAR(50)   ENCODE zstd
	,order_id VARCHAR(50)   ENCODE zstd
	,pocket_id VARCHAR(50)   ENCODE zstd
	,da_track VARCHAR(50)   ENCODE zstd
	,da_principal_amount VARCHAR(50)   ENCODE zstd
	,is_loan_request VARCHAR(50)   ENCODE zstd
	,migrated_ortel_subscriber_flag VARCHAR(50)   ENCODE zstd
	,reward_purchased VARCHAR(50)   ENCODE zstd
	,feebuffer VARCHAR(50)   ENCODE lzo
	,tax_buffer VARCHAR(50)   ENCODE lzo
	,paymentgateway VARCHAR(50)   ENCODE zstd
	,zipcode VARCHAR(50)   ENCODE lzo
	,professional_channel_name VARCHAR(50)   ENCODE lzo
	,topupcriteriaflag VARCHAR(50)   ENCODE zstd
	,nus_id VARCHAR(50)   ENCODE lzo
	,nus_applied VARCHAR(50)   ENCODE zstd
	,slotid VARCHAR(50)   ENCODE lzo
	,reserved_bundle_cost VARCHAR(50)   ENCODE lzo
	,created_date TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE zstd
	,filename VARCHAR(50) NOT NULL DEFAULT 'nofilename'::character varying ENCODE RAW
)
DISTSTYLE EVEN
 SORTKEY (
	filename
	)
;

---#### RRBS DATA CDR tables CDRTYPE,serviceid and subscribertype----------------------------------

CREATE TABLE IF NOT EXISTS uk_rrbs_stg.stg_rrbs_data_cdrtype
(
	param_id SMALLINT   ENCODE lzo
	,param_val VARCHAR(100)   ENCODE lzo
	,param_desc VARCHAR(250)   ENCODE lzo
	,created_date DATE NOT NULL
	,filename VARCHAR(100)   ENCODE lzo
)
DISTSTYLE ALL
;

CREATE TABLE IF NOT EXISTS uk_rrbs_stg.stg_rrbs_data_serviceid
(
	--sk_serviceid INTEGER  DEFAULT default_identity(138555, 0, '1,1'::text) NULL ENCODE lzo,
	param_id SMALLINT   ENCODE lzo
	,param_val VARCHAR(100)   ENCODE lzo
	,param_desc VARCHAR(250)   ENCODE lzo
	,created_date DATE NOT NULL
	,filename VARCHAR(100)   ENCODE lzo
)
DISTSTYLE ALL
;


CREATE TABLE IF NOT EXISTS uk_rrbs_stg.stg_rrbs_data_subscribertype
(
	--sk_subscribertype INTEGER  DEFAULT default_identity(138563, 0, '1,1'::text) NULL ENCODE lzo,
	param_id SMALLINT   ENCODE lzo
	,param_val VARCHAR(100)   ENCODE lzo
	,param_desc VARCHAR(250)   ENCODE lzo
	,created_date DATE NOT NULL
	,filename VARCHAR(100)   ENCODE lzo
)
DISTSTYLE ALL
;

---#### RRBS SMS CDR tables CDRTYPE,serviceid, subscribertype, buckettype, calltype & smsfeature----------------------------------

CREATE TABLE IF NOT EXISTS uk_rrbs_stg.stg_rrbs_sms_buckettype
(
	--sk_buckettype INTEGER  DEFAULT default_identity(138547, 0, '1,1'::text) NULL ENCODE lzo,
	param_id SMALLINT   ENCODE lzo
	,param_val VARCHAR(100)   ENCODE lzo
	,param_desc VARCHAR(250)   ENCODE lzo
	,created_date DATE NOT NULL
	,filename VARCHAR(100)   ENCODE lzo
)
DISTSTYLE ALL
;

CREATE TABLE IF NOT EXISTS uk_rrbs_stg.stg_rrbs_sms_calltype
(
	--sk_calltype INTEGER  DEFAULT default_identity(138531, 0, '1,1'::text) NULL ENCODE lzo,
	param_id SMALLINT   ENCODE lzo
	,param_val VARCHAR(100)   ENCODE lzo
	,param_desc VARCHAR(250)   ENCODE lzo
	,created_date DATE NOT NULL
	,filename VARCHAR(100)   ENCODE lzo
)
DISTSTYLE ALL
;

CREATE TABLE IF NOT EXISTS uk_rrbs_stg.stg_rrbs_sms_cdrtype
(
	--sk_cdr_type INTEGER  DEFAULT default_identity(138527, 0, '1,1'::text) NULL ENCODE lzo,
	param_id SMALLINT   ENCODE lzo
	,param_val VARCHAR(100)   ENCODE lzo
	,param_desc VARCHAR(250)   ENCODE lzo
	,created_date DATE NOT NULL
	,filename VARCHAR(100)   ENCODE lzo
)
DISTSTYLE ALL
;



CREATE TABLE IF NOT EXISTS uk_rrbs_stg.stg_rrbs_sms_serviceid
(
	--sk_serviceid INTEGER  DEFAULT default_identity(138535, 0, '1,1'::text) NULL ENCODE lzo,
	param_id SMALLINT   ENCODE lzo
	,param_val VARCHAR(100)   ENCODE lzo
	,param_desc VARCHAR(250)   ENCODE lzo
	,created_date DATE NOT NULL
	,filename VARCHAR(100)   ENCODE lzo
)
DISTSTYLE ALL
;

CREATE TABLE IF NOT EXISTS uk_rrbs_stg.stg_rrbs_sms_smsfeature
(
	--sk_smsfeature INTEGER  DEFAULT default_identity(138543, 0, '1,1'::text) NULL ENCODE lzo,
	param_id SMALLINT   ENCODE lzo
	,param_val VARCHAR(100)   ENCODE lzo
	,param_desc VARCHAR(250)   ENCODE lzo
	,created_date DATE NOT NULL
	,filename VARCHAR(100)   ENCODE lzo
)
DISTSTYLE ALL
;

CREATE TABLE IF NOT EXISTS uk_rrbs_stg.stg_rrbs_sms_subscribertype
(
	--sk_subscribertype INTEGER  DEFAULT default_identity(138495, 0, '1,1'::text) NULL ENCODE lzo,
	param_id SMALLINT   ENCODE lzo
	,param_val VARCHAR(100)   ENCODE lzo
	,param_desc VARCHAR(250)   ENCODE lzo
	,created_date DATE NOT NULL
	,filename VARCHAR(100)   ENCODE lzo
)
DISTSTYLE ALL
;

---#### RRBS VOICE CDR tables CDRTYPE,serviceid, subscribertype, buckettype, calltype,failcause & callfeature----------------------------------

CREATE TABLE IF NOT EXISTS uk_rrbs_stg.stg_rrbs_voice_cdrtype
(
	param_id SMALLINT   ENCODE lzo
	,param_val VARCHAR(100)   ENCODE lzo
	,param_desc VARCHAR(250)   ENCODE lzo
	,created_date DATE NOT NULL
	,filename VARCHAR(100)   ENCODE lzo
)
DISTSTYLE ALL
;

CREATE TABLE IF NOT EXISTS uk_rrbs_stg.stg_rrbs_voice_serviceid
(
	--sk_serviceid INTEGER  DEFAULT default_identity(138555, 0, '1,1'::text) NULL ENCODE lzo,
	param_id SMALLINT   ENCODE lzo
	,param_val VARCHAR(100)   ENCODE lzo
	,param_desc VARCHAR(250)   ENCODE lzo
	,created_date DATE NOT NULL
	,filename VARCHAR(100)   ENCODE lzo
)
DISTSTYLE ALL
;



CREATE TABLE IF NOT EXISTS uk_rrbs_stg.stg_rrbs_voice_subscribertype
(
	--sk_subscribertype INTEGER  DEFAULT default_identity(138563, 0, '1,1'::text) NULL ENCODE lzo,
	param_id SMALLINT   ENCODE lzo
	,param_val VARCHAR(100)   ENCODE lzo
	,param_desc VARCHAR(250)   ENCODE lzo
	,created_date DATE NOT NULL
	,filename VARCHAR(100)   ENCODE lzo
)
DISTSTYLE ALL
;

CREATE TABLE IF NOT EXISTS uk_rrbs_stg.stg_rrbs_voice_calltype
(
	--sk_calltype INTEGER  DEFAULT default_identity(138531, 0, '1,1'::text) NULL ENCODE lzo,
	param_id SMALLINT   ENCODE lzo
	,cdrtype_id SMALLINT   ENCODE lzo
	,param_val VARCHAR(100)   ENCODE lzo
	,param_desc VARCHAR(250)   ENCODE lzo
	,created_date DATE NOT NULL
	,filename VARCHAR(100)   ENCODE lzo
)
DISTSTYLE ALL
;

CREATE TABLE IF NOT EXISTS uk_rrbs_stg.stg_rrbs_voice_buckettype
(
	--sk_buckettype INTEGER  DEFAULT default_identity(138547, 0, '1,1'::text) NULL ENCODE lzo,
	param_id SMALLINT   ENCODE lzo
	,param_val VARCHAR(100)   ENCODE lzo
	,param_desc VARCHAR(250)   ENCODE lzo
	,created_date DATE NOT NULL
	,filename VARCHAR(100)   ENCODE lzo
)
DISTSTYLE ALL
;

CREATE TABLE IF NOT EXISTS uk_rrbs_stg.stg_rrbs_voice_callfeature
(
	--sk_callfeature INTEGER  DEFAULT default_identity(138547, 0, '1,1'::text) NULL ENCODE lzo,
	param_id SMALLINT   ENCODE lzo
	,param_val VARCHAR(100)   ENCODE lzo
	,param_desc VARCHAR(250)   ENCODE lzo
	,created_date DATE NOT NULL
	,filename VARCHAR(100)   ENCODE lzo
)
DISTSTYLE ALL
;

CREATE TABLE IF NOT EXISTS uk_rrbs_stg.stg_rrbs_voice_failcause
(
	--sk_failcause INTEGER  DEFAULT default_identity(138547, 0, '1,1'::text) NULL ENCODE lzo,
	param_id SMALLINT   ENCODE lzo
	,param_val VARCHAR(100)   ENCODE lzo
	,param_desc VARCHAR(250)   ENCODE lzo
	,created_date DATE NOT NULL
	,filename VARCHAR(100)   ENCODE lzo
)
DISTSTYLE ALL
;

---#### RRBS TOPUP CDR tables operationcode,serviceid, & bucketCategory----------------------------------

CREATE TABLE IF NOT EXISTS uk_rrbs_stg.stg_rrbs_topup_operationcode
(
	param_id SMALLINT   ENCODE lzo
	,param_val VARCHAR(100)   ENCODE lzo
	,param_desc VARCHAR(250)   ENCODE lzo
	,created_date DATE NOT NULL
	,filename VARCHAR(100)   ENCODE lzo
)
DISTSTYLE ALL
;

CREATE TABLE IF NOT EXISTS uk_rrbs_stg.stg_rrbs_topup_serviceid
(
	--sk_serviceid INTEGER  DEFAULT default_identity(138555, 0, '1,1'::text) NULL ENCODE lzo,
	param_id SMALLINT   ENCODE lzo
	,param_val VARCHAR(100)   ENCODE lzo
	,param_desc VARCHAR(250)   ENCODE lzo
	,created_date DATE NOT NULL
	,filename VARCHAR(100)   ENCODE lzo
)
DISTSTYLE ALL
;


CREATE TABLE IF NOT EXISTS uk_rrbs_stg.stg_rrbs_topup_bundleCategory
(
	--sk_bundleCategory INTEGER  DEFAULT default_identity(138563, 0, '1,1'::text) NULL ENCODE lzo,
	param_id SMALLINT   ENCODE lzo
	,param_val VARCHAR(100)   ENCODE lzo
	,param_desc VARCHAR(250)   ENCODE lzo
	,created_date DATE NOT NULL
	,filename VARCHAR(100)   ENCODE lzo
)
DISTSTYLE ALL
;

---#### RRBS SMS, Data, Voice & Topup CodeText Script----------------------------------

CREATE TABLE IF NOT EXISTS uk_rrbs_stg.stg_rrbs_voice_CodeText
(
	--sk_CodeText INTEGER  DEFAULT default_identity(138563, 0, '1,1'::text) NULL ENCODE lzo,
	code_param VARCHAR(100)   ENCODE lzo
	,param_id SMALLINT   ENCODE lzo
	,param_val VARCHAR(100)   ENCODE lzo
	,param_desc VARCHAR(250)   ENCODE lzo
	,created_date DATE NOT NULL
	,filename VARCHAR(100)   ENCODE lzo
)
DISTSTYLE ALL
;

CREATE TABLE IF NOT EXISTS uk_rrbs_stg.stg_rrbs_sms_CodeText
(
	--sk_CodeText INTEGER  DEFAULT default_identity(138563, 0, '1,1'::text) NULL ENCODE lzo,
	code_param VARCHAR(100)   ENCODE lzo
	,param_id SMALLINT   ENCODE lzo
	,param_val VARCHAR(100)   ENCODE lzo
	,param_desc VARCHAR(250)   ENCODE lzo
	,created_date DATE NOT NULL
	,filename VARCHAR(100)   ENCODE lzo
)
DISTSTYLE ALL
;

CREATE TABLE IF NOT EXISTS uk_rrbs_stg.stg_rrbs_data_CodeText
(
	--sk_CodeText INTEGER  DEFAULT default_identity(138563, 0, '1,1'::text) NULL ENCODE lzo,
	code_param VARCHAR(100)   ENCODE lzo
	,param_id SMALLINT   ENCODE lzo
	,param_val VARCHAR(100)   ENCODE lzo
	,param_desc VARCHAR(250)   ENCODE lzo
	,created_date DATE NOT NULL
	,filename VARCHAR(100)   ENCODE lzo
)
DISTSTYLE ALL
;


CREATE TABLE IF NOT EXISTS uk_rrbs_stg.stg_rrbs_topup_CodeText
(
	--sk_CodeText INTEGER  DEFAULT default_identity(138563, 0, '1,1'::text) NULL ENCODE lzo,
	code_param VARCHAR(100)   ENCODE lzo
	,param_id SMALLINT   ENCODE lzo
	,param_val VARCHAR(100)   ENCODE lzo
	,param_desc VARCHAR(250)   ENCODE lzo
	,created_date DATE NOT NULL
	,filename VARCHAR(100)   ENCODE lzo
)
DISTSTYLE ALL
;
