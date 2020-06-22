CREATE TABLE IF NOT EXISTS fact_rrbs_gprs
(
	sk_rrbs_gprs BIGINT  DEFAULT default_identity(1, 0, '1,1'::text) ENCODE zstd
	,cdr_type SMALLINT   ENCODE az64
	,network_id SMALLINT   ENCODE az64
	,data_feature SMALLINT   ENCODE az64
	,tariffplan_id SMALLINT   ENCODE az64
	,service_id SMALLINT   ENCODE az64
	,msisdn BIGINT   ENCODE az64
	,apn VARCHAR(100)   ENCODE zstd
	,pdp_address VARCHAR(50)   ENCODE zstd
	,cellid VARCHAR(50)   ENCODE zstd
	,imei VARCHAR(50)   ENCODE zstd
	,imsi BIGINT   ENCODE az64
	,serving_node VARCHAR(30)   ENCODE zstd
	,ggsn_address VARCHAR(50)   ENCODE zstd
	,roaming_zone_name VARCHAR(30)   ENCODE zstd
	,roam_flag SMALLINT   ENCODE az64
	,granted_bytes_cumulative BIGINT   ENCODE az64
	,granted_money NUMERIC(22,6)   ENCODE az64
	,total_used_bytes BIGINT   ENCODE az64
	,chargeable_used_bytes BIGINT   ENCODE az64
	,uploaded_bytes BIGINT   ENCODE az64
	,downloaded_bytes BIGINT   ENCODE az64
	,data_connection_time TIMESTAMP WITHOUT TIME ZONE   ENCODE zstd
	,data_termination_time TIMESTAMP WITHOUT TIME ZONE   ENCODE zstd
	,time_duration INTEGER   ENCODE az64
	,initial_account_balance NUMERIC(22,6)   ENCODE az64
	,data_charge NUMERIC(22,6)   ENCODE az64
	,final_account_balance NUMERIC(22,6)   ENCODE az64
	,free_bytes BIGINT   ENCODE az64
	,sessionid VARCHAR(50)   ENCODE zstd
	,last_topup_type SMALLINT   ENCODE az64
	,free_data_expiry_DATE DATE   ENCODE zstd
	,charge_indicator SMALLINT   ENCODE az64
	,final_unit_indicator SMALLINT   ENCODE az64
	,bundle_code VARCHAR(10)   ENCODE RAW
	,accid VARCHAR(11)   ENCODE zstd
	,deptid VARCHAR(15)   ENCODE zstd
	,bundle_used_data VARCHAR(10)   ENCODE zstd
	,bundle_data_charge NUMERIC(22,6)   ENCODE az64
	,bundle_balance NUMERIC(22,6)   ENCODE az64
	,bundle_plan_id VARCHAR(5)   ENCODE zstd
	,local_roam_country_code SMALLINT   ENCODE az64
	,sdfid SMALLINT   ENCODE az64
	,rategroupid SMALLINT   ENCODE az64
	,serviceid VARCHAR(4)   ENCODE zstd
	,cdr_time_stamp TIMESTAMP WITHOUT TIME ZONE   ENCODE zstd
	,INTial_free_units BIGINT   ENCODE az64
	,subscriber_type SMALLINT   ENCODE az64
	,cdr_sequence_number INTEGER   ENCODE az64
	,sub_acct_id BIGINT   ENCODE az64
	,ggsn_time VARCHAR(100)   ENCODE zstd
	,tariff_plan_change_id SMALLINT   ENCODE az64
	,used_unit_bfr_tariff_plan_change BIGINT   ENCODE az64
	,family_account_id INTEGER   ENCODE az64
	,bundle_version_name VARCHAR(50)   ENCODE zstd
	,bundle_versionid SMALLINT   ENCODE az64
	,reward_slab_id SMALLINT   ENCODE az64
	,pricing_plan_id SMALLINT   ENCODE az64
	,rate_id INTEGER   ENCODE az64
	,pocket_id INTEGER   ENCODE az64
	,consumed_promo_amount NUMERIC(22,6)   ENCODE az64
	,consumed_surcharge NUMERIC(22,6)   ENCODE az64
	,rlh_charging_indicator SMALLINT   ENCODE az64
	,granted_bytes_current BIGINT   ENCODE az64
	,roaming_partner_id SMALLINT   ENCODE az64	
--free text columns
	,fct_free_text_1	VARCHAR(50)	ENCODE zstd
	,fct_free_text_2	VARCHAR(50)	ENCODE zstd
	,fct_free_text_3	VARCHAR(50)	ENCODE zstd
	,fct_free_text_4	VARCHAR(50)	ENCODE zstd
	,fct_free_text_5	VARCHAR(50)	ENCODE zstd
	,fct_free_text_6	VARCHAR(50)	ENCODE zstd
	,fct_free_text_7	VARCHAR(50)	ENCODE zstd
	,fct_free_text_8	VARCHAR(50)	ENCODE zstd
	,fct_free_text_9	VARCHAR(50)	ENCODE zstd
	,fct_free_text_10	VARCHAR(50)	ENCODE zstd
--additional metadata
	,data_connection_month INTEGER   ENCODE RAW    --yyyymm
    ,data_connection_dt DATE   ENCODE RAW			--yyyymmdd
	,data_connection_dt_num INTEGER   ENCODE az64	--yyyymmdd	
	,data_connection_hour  INTEGER   ENCODE az64	
	,data_termination_month INTEGER   ENCODE az64    --yyyymm
	,data_termination_dt DATE   ENCODE RAW			--yyyymmdd
	,data_termination_dt_num INTEGER   ENCODE az64	--yyyymmdd
	,data_termination_hour  INTEGER   ENCODE az64		
--gmt additions
	,data_connection_month_gmt INTEGER ENCODE az64	
	,data_connection_dt_num_gmt INTEGER   ENCODE az64	--yyyymmdd		
	,data_connection_hour_gmt  INTEGER   ENCODE az64	
	,data_connection_time_gmt TIMESTAMP WITHOUT TIME ZONE   ENCODE zstd	
	,data_termination_month_gmt  INTEGER ENCODE az64	
	,data_termination_dt_num_gmt INTEGER   ENCODE az64	--yyyymmdd	
	,data_termination_hour_gmt  INTEGER   ENCODE az64
	,data_termination_time_gmt TIMESTAMP WITHOUT TIME ZONE   ENCODE zstd	
	,total_bytes_mb BIGINT   ENCODE az64 --uploaded + downloaded
	,free_data_expiry_DATE_num  INTEGER   ENCODE az64
	,filename VARCHAR(100)   ENCODE zstd
	,batch_id INTEGER NOT NULL  ENCODE zstd
	,created_DATE TIMESTAMP WITHOUT TIME ZONE NOT NULL  ENCODE zstd
	,rec_checksum varchar(32) ENCODE zstd
)
DISTSTYLE KEY
DISTKEY (imsi)
SORTKEY (
	 data_connection_month
	 ,data_connection_dt_num
	,bundle_code
	)
;