---#### 1. RRBS Voice table :"stg_rrbs_voice"----------------------------------
CREATE TABLE uk_rrbs_stg.stg_rrbs_voice(
	Voice_call_cdr int NULL ENCODE lzo,
	Network_ID varchar(10) NULL ENCODE lzo,
	Call_Type varchar(10) NULL ENCODE lzo,
	call_feature varchar(10) NULL ENCODE lzo,
	TariffPlan_ID int NULL ENCODE lzo,
	Service_ID int NULL ENCODE lzo,
	cli varchar(20) NULL ENCODE lzo,
	Dialed_number varchar(50) NULL ENCODE lzo,
	Charged_party_number varchar(20) NULL ENCODE lzo,
	IMSI varchar(50) NULL ENCODE lzo,
	Serving_Node varchar(30) NULL ENCODE lzo,
	MCC varchar(50) NULL ENCODE lzo,
	MNC varchar(50) NULL ENCODE lzo,
	LAC varchar(50) NULL ENCODE lzo,
	Calling_Cell_ID varchar(30) NULL ENCODE lzo,
	CellZone_Code varchar(30) NULL ENCODE lzo,
	CellZone_Name varchar(30) NULL ENCODE lzo,
	Original_Dialed_number varchar(30) NULL ENCODE lzo,
	Destination_zone varchar(30) NULL ENCODE lzo,
	Destination_area_code varchar(30) NULL ENCODE lzo,
	Destinationzone_name varchar(30) NULL ENCODE lzo,
	Roaming_area_code varchar(20) NULL ENCODE lzo,
	Roaming_zone_name varchar(30) NULL ENCODE lzo,
	roam_flag varchar(10)  NULL ENCODE lzo,
	Roam_area_number varchar(10) NULL ENCODE lzo,
	Granted_Time varchar(10) NULL ENCODE lzo,
	Granted_Money decimal(10, 4) NULL ENCODE lzo,
	call_duration varchar(10) NULL ENCODE lzo,
	Chargeable_Used_Time varchar(30) NULL ENCODE lzo,
	Call_date varchar(30) NULL ENCODE lzo,
	Call_termination_time varchar(30) NULL ENCODE lzo,
	Initial_Account_balance decimal(10, 4) NULL ENCODE lzo,
	Talk_charge decimal(10, 4) NULL ENCODE lzo,
	balance decimal(10, 4) NULL ENCODE lzo,
	Free_minutes_account_balance decimal(20, 6) NULL ENCODE lzo,
	Account_Id int NULL ENCODE lzo,
	Dept_Id int NULL ENCODE lzo,
	Free_Zone_Id varchar(50) NULL ENCODE lzo,
	Trace_Id varchar(50) NULL ENCODE lzo,
	Last_Topup_Type varchar(1) NULL ENCODE lzo,
	Bundle_Code varchar(10) NULL ENCODE lzo,
	Free_Zone_Expiry_Date varchar(15) NULL ENCODE lzo,
	Ringing_Duration varchar(3) NULL ENCODE lzo,
	Ring_Indicator varchar(1) NULL ENCODE lzo,
	National_Bundle_Code varchar(10) NULL ENCODE lzo,
	National_Used_Minutes varchar(10) NULL ENCODE lzo,
	National_Charge varchar(10) NULL ENCODE lzo,
	Pool_Number varchar(20) NULL ENCODE lzo,
	Bundle_used_seconds varchar(10) NULL ENCODE lzo,
	Bundle_Call_Charge varchar(10) NULL ENCODE lzo,
	Bundle_Balance varchar(10) NULL ENCODE lzo,
	Bundle_Plan_ID varchar(5) NULL ENCODE lzo,
	Final_Unit_Indicator varchar(1) NULL ENCODE lzo,
	NCALL_Free_Units varchar(10) NULL ENCODE lzo,
	Local_Roam_Country_Code varchar(4) NULL ENCODE lzo,
	CDR_Time_Stamp varchar(50) NULL ENCODE lzo,
	Bucket_type varchar(50) NULL ENCODE lzo,
	Conversion_Unit varchar(50) NULL ENCODE lzo,
	Intial_Free_units varchar(50) NULL ENCODE lzo,
	Subscriber_Type varchar(50) NULL ENCODE lzo,
	Call_Forwarding_Indicator varchar(50) NULL ENCODE lzo,
	CDR_Sequence_Number varchar(50) NULL ENCODE lzo,
	Sub_Acct_Id varchar(50) NULL ENCODE lzo,
	Announcement_Time varchar(50) NULL ENCODE lzo,
	Family_id varchar(50) NULL ENCODE lzo,
	Idp_Time varchar(100) NULL ENCODE lzo,
	CS_free_Mins varchar(50) NULL ENCODE lzo,
	Free_Units_Detected varchar(50) NULL ENCODE lzo,
	Multileg_Charging_Flag varchar(50) NULL ENCODE lzo,
	Extension_Record varchar(50) NULL ENCODE lzo,
	Connect_Number varchar(50) NULL ENCODE lzo,
	Dialed_Number1 varchar(50) NULL ENCODE lzo,
	Service_Description varchar(100) NULL ENCODE lzo,
	Alternate_Plan_Id varchar(50) NULL ENCODE lzo,
	Serving_Node_Name varchar(50) NULL ENCODE lzo,
	Fixed_charge varchar(50) NULL ENCODE lzo,
	Fixed_charge_Resource_Impact varchar(50) NULL ENCODE lzo,
	Bundle_Version_Name varchar(50) NULL ENCODE lzo,
	FLH_Reward_slab_id varchar(50) NULL ENCODE lzo,
	Bundle_version_ID varchar(50) NULL ENCODE lzo,
	Rate_ID varchar(50) NULL ENCODE lzo,
	Pricing_Plan_ID varchar(50) NULL ENCODE lzo,
	Pocket_Id varchar(50) NULL ENCODE lzo,
	Consumed_Promo_Amount varchar(50) NULL ENCODE lzo,
	Pervious_call_trace_Id varchar(50) NULL ENCODE lzo,
	Termination_Cause varchar(50) NULL ENCODE lzo,
	Non_Chargeable_Unit varchar(50) NULL ENCODE lzo,
	Call_Forward_MSISDN varchar(50) NULL ENCODE lzo,
	Service_Type varchar(50) NULL ENCODE lzo,
	Used_time_in_previous_grant varchar(50) NULL ENCODE lzo,
	RLH_Charging_Indicator varchar(50) NULL ENCODE lzo,
	Surcharge_Consumed varchar(50) NULL ENCODE lzo,
	Excess_Used_units_received varchar(50) NULL ENCODE lzo,
	Failcause varchar(50) NULL ENCODE lzo,
	Roaming_Partner_ID varchar(50) NULL ENCODE lzo
);

---#### 2. RRBS GPRS table :"stg_rrbs_gprs"----------------------------------

CREATE TABLE uk_rrbs_stg.stg_rrbs_gprs(
	CDR_Type int NULL ENCODE lzo,
	Network_ID int NULL ENCODE lzo,
	Data_Feature int NULL ENCODE lzo,
	TariffPlan_ID int NULL ENCODE lzo,
	Service_ID int NULL ENCODE lzo,
	Msisdn varchar(20) NULL ENCODE lzo,
	APN varchar(100) NULL ENCODE lzo,
	PDP_Address varchar(50) NULL ENCODE lzo,
	cellId varchar(50) NULL ENCODE lzo,
	IMEI varchar(50) NULL ENCODE lzo,
	IMSI varchar(20) NULL ENCODE lzo,
	Serving_Node varchar(30) NULL ENCODE lzo,
	GGSN_Address varchar(50) NULL ENCODE lzo,
	Roaming_zone_name varchar(30) NULL ENCODE lzo,
	Roam_Flag int NULL ENCODE lzo,
	Granted_bytes_cumulative bigint NULL ENCODE lzo,
	Granted_Money decimal(20, 6) NULL ENCODE lzo,
	Total_Used_Bytes bigint NULL ENCODE lzo,
	Chargeable_Used_Bytes bigint NULL ENCODE lzo,
	Uploaded_Bytes int NULL ENCODE lzo,
	Downloaded_Bytes int NULL ENCODE lzo,
	Data_connection_time varchar(20) NULL ENCODE lzo,
	Data_termination_time varchar(20) NULL ENCODE lzo,
	Time_Duration varchar(20) NULL ENCODE lzo,
	Initial_Account_balance decimal(20, 6) NULL ENCODE lzo,
	Data_charge decimal(20, 6) NULL ENCODE lzo,
	Final_Account_balance decimal(20, 6) NULL ENCODE lzo,
	Free_Bytes varchar(50) NULL ENCODE lzo,
	Sessionid varchar(50) NULL ENCODE lzo,
	Last_Topup_Type varchar(50) NULL ENCODE lzo,
	Free_Data_Expiry_Date varchar(10) NULL ENCODE lzo,
	Charge_Indicator varchar(50) NULL ENCODE lzo,
	Final_unit_indicator varchar(50) NULL ENCODE lzo,
	Bundle_Code varchar(10) NULL ENCODE lzo,
	AccId varchar(11) NULL ENCODE lzo,
	DeptId varchar(15) NULL ENCODE lzo,
	Bundle_used_data varchar(10) NULL ENCODE lzo,
	Bundle_data_Charge varchar(10) NULL ENCODE lzo,
	Bundle_balance varchar(10) NULL ENCODE lzo,
	Bundle_Plan_ID varchar(5) NULL ENCODE lzo,
	Local_Roam_Country_Code varchar(4) NULL ENCODE lzo,
	sdfId varchar(100) NULL ENCODE lzo,
	RateGroupID varchar(4) NULL ENCODE lzo,
	ServiceID varchar(4) NULL ENCODE lzo,
	CDR_Time_Stamp varchar(20) NULL ENCODE lzo,
	Intial_Free_units varchar(50) NULL ENCODE lzo,
	Subscriber_Type varchar(50) NULL ENCODE lzo,
	CDR_Sequence_Number varchar(50) NULL ENCODE lzo,
	Sub_Acct_Id varchar(50) NULL ENCODE lzo,
	GGSN_Time varchar(100) NULL ENCODE lzo,
	Tariff_Plan_Change_ID varchar(50) NULL ENCODE lzo,
	Used_Units_Before_Tariff_Plan_Change varchar(50) NULL ENCODE lzo,
	Family_Account_ID varchar(50) NULL ENCODE lzo,
	Bundle_Version_Name varchar(50) NULL ENCODE lzo,
	Bundle_VersionId varchar(50) NULL ENCODE lzo,
	Reward_slab_Id varchar(50) NULL ENCODE lzo,
	Pricing_Plan_ID varchar(50) NULL ENCODE lzo,
	Rate_ID varchar(50) NULL ENCODE lzo,
	Pocket_Id varchar(50) NULL ENCODE lzo,
	Consumed_Promo_Amount varchar(50) NULL ENCODE lzo,
	Consumed_Surcharge varchar(50) NULL ENCODE lzo,
	RLH_Charging_Indicator varchar(50) NULL ENCODE lzo,
	Granted_bytes_current varchar(50) NULL ENCODE lzo,
	Roaming_Partner_ID varchar(50) NULL ENCODE lzo
);

---#### 3. RRBS SMS table :"stg_rrbs_sms"------------------------------------

CREATE TABLE uk_rrbs_stg.stg_rrbs_sms(
	CDR_Types varchar(40) NULL ENCODE lzo,
	Network_ID varchar(10) NULL ENCODE lzo,
	Call_Type varchar(5) NULL ENCODE lzo,
	Plan_Id int NULL ENCODE lzo,
	Service_Id int NULL ENCODE lzo,
	cli varchar(20) NULL ENCODE lzo,
	Dialed_number varchar(50) NULL ENCODE lzo,
	IMSI varchar(30) NULL ENCODE lzo,
	Serving_Node varchar(30) NULL ENCODE lzo,
	Destination_area_code varchar(50) NULL ENCODE lzo,
	Destination_zone_code varchar(50) NULL ENCODE lzo,
	Destination_zone_name varchar(50) NULL ENCODE lzo,
	Roam_Flag varchar(50) NULL ENCODE lzo,
	Roaming_node varchar(50) NULL ENCODE lzo,
	Roaming_area_code varchar(50) NULL ENCODE lzo,
	Roaming_zone_name varchar(50) NULL ENCODE lzo,
	SMS_Feature varchar(50) NULL ENCODE lzo,
	Number_of_SMS_charged varchar(10) NULL ENCODE lzo,
	Number_of_free_SMS varchar(20) NULL ENCODE lzo,
	Initial_Account_balance decimal(10, 4) NULL ENCODE lzo,
	msg_cost decimal(10, 4) NULL ENCODE lzo,
	balance decimal(10, 4) NULL ENCODE lzo,
	Free_SMS_account_balance varchar(100) NULL ENCODE lzo,
	Instance_ID_Session_ID varchar(20) NULL ENCODE lzo,
	msg_date varchar(21) NOT NULL ENCODE lzo,
	Account_Id int NULL ENCODE lzo,
	Dept_Id int NULL ENCODE lzo,
	Free_Zone_Id varchar(10) NULL ENCODE lzo,
	Last_Topup_Type varchar(1) NULL ENCODE lzo,
	Bundle_Code varchar(6) NULL ENCODE lzo,
	Free_Zone_Expiry_Date varchar(10) NULL ENCODE lzo,
	Bundle_SMS_Charge varchar(10) NULL ENCODE lzo,
	Bundle_Balance varchar(10) NULL ENCODE lzo,
	Bundle_Plan_ID varchar(5) NULL ENCODE lzo,
	NSMS_Free_Units varchar(10) NULL ENCODE lzo,
	Local_Roam_Country_Code varchar(4) NULL ENCODE lzo,
	CDR_Time_Stamp varchar(21) NULL ENCODE lzo,
	Bucket_type varchar(50) NULL ENCODE lzo,
	Intial_Free_units varchar(50) NULL ENCODE lzo,
	Subscriber_Type varchar(50) NULL ENCODE lzo,
	Sub_Acct_Id varchar(50) NULL ENCODE lzo,
	Family_id varchar(50) NULL ENCODE lzo,
	Concat_Message_Flag varchar(50) NULL ENCODE lzo,
	Concat_Message_Reference_Number varchar(50) NULL ENCODE lzo,
	Concat_Message_Total_chunks varchar(50) NULL ENCODE lzo,
	Concat_Message_Current_chunk varchar(50) NULL ENCODE lzo,
	Trace_Id varchar(50) NULL ENCODE lzo,
	Idp_Time varchar(50) NULL ENCODE lzo,
	Extension_Record varchar(50) NULL ENCODE lzo,
	Call_Indicator varchar(50) NULL ENCODE lzo,
	Serving_Node_Name varchar(50) NULL ENCODE lzo,
	Bundle_Version_Name varchar(50) NULL ENCODE lzo,
	FLH_Reward_slab_id varchar(50) NULL ENCODE lzo,
	Bundle_version_ID varchar(50) NULL ENCODE lzo,
	Rate_ID varchar(50) NULL ENCODE lzo,
	Pricing_Plan_ID varchar(50) NULL ENCODE lzo,
	Pocket_Id varchar(50) NULL ENCODE lzo,
	Consumed_Promo_Amount varchar(50) NULL ENCODE lzo,
	RLH_Charging_Indicator varchar(50) NULL ENCODE lzo,
	Surcharge_Consumed varchar(50) NULL ENCODE lzo,
	Roaming_Partner_ID varchar(50) NULL ENCODE lzo
);


---#### 4. RRBS TOPUP table :"stg_rrbs_topup"--------------------------------

CREATE TABLE uk_rrbs_stg.stg_rrbs_topup(
	Operation_Code varchar(10) NULL ENCODE lzo,
	Network_ID varchar(50) NULL ENCODE lzo,
	recharge_type varchar(10) NOT NULL ENCODE lzo,
	MSISDN varchar(50) NULL ENCODE lzo,
	IMSI varchar(30) NULL ENCODE lzo,
	account_pin_number varchar(50) NULL ENCODE lzo,
	Voucher_card_ID varchar(50) NULL ENCODE lzo,
	Special_topup_amount decimal(10, 4) NULL ENCODE lzo,
	Transaction_ID varchar(50) NULL ENCODE lzo,
	new_balance decimal(10, 4) NULL ENCODE lzo,
	Face_value decimal(10, 4) NULL ENCODE lzo,
	rechage_amount decimal(10, 4) NULL ENCODE lzo,
	Promo_Validity_Date varchar(50) NULL ENCODE lzo,
	Free_minutes varchar(20) NULL ENCODE lzo,
	Free_Voucher_Onnet_Minutes varchar(20) NULL ENCODE lzo,
	Free_minutes_expiry_date varchar(20) NULL ENCODE lzo,
	Free_SMS varchar(30) NULL ENCODE lzo,
	Free_Voucher_Onnet_SMS varchar(20) NULL ENCODE lzo,
	Free_SMS_expiry_date varchar(20) NULL ENCODE lzo,
	Free_Offnet_Minutes varchar(20) NULL ENCODE lzo,
	Free_Voucher_Offnet_Minutes varchar(20) NULL ENCODE lzo,
	Free_Offnet_Minutes_Expiry_Date varchar(20) NULL ENCODE lzo,
	Free_Offnet_SMS varchar(20) NULL ENCODE lzo,
	Free_Voucher_Offnet_SMS varchar(20) NULL ENCODE lzo,
	Free_Offnet_SMS_Expiry_Date varchar(20) NULL ENCODE lzo,
	Free_OffNet2_Minutes varchar(20) NULL ENCODE lzo,
	Free_VoucherCard_Offnet2_Minutes varchar(20) NULL ENCODE lzo,
	Free_OffNet2_Minutes_expiry_date varchar(20) NULL ENCODE lzo,
	Free_OffNet2_SMS varchar(20) NULL ENCODE lzo,
	Free_VoucherCard_Offnet2_SMS varchar(20) NULL ENCODE lzo,
	Free_OffNet2_SMS_expiry_date varchar(20) NULL ENCODE lzo,
	Free_OffNet3_Minutes varchar(20) NULL ENCODE lzo,
	Free_VoucherCard_Offnet3_Minutes varchar(20) NULL ENCODE lzo,
	Free_OffNet3_Minutes_expiry_date varchar(20) NULL ENCODE lzo,
	Free_OffNet3_SMS varchar(20) NULL ENCODE lzo,
	Free_VoucherCard_Offnet3_SMS varchar(20) NULL ENCODE lzo,
	Free_OffNet3_SMS_expiry_date varchar(20) NULL ENCODE lzo,
	Free_Data varchar(20) NULL ENCODE lzo,
	Free_Voucher_Data varchar(20) NULL ENCODE lzo,
	Free_Data_ExpiryDate varchar(20) NULL ENCODE lzo,
	Account_validity_date varchar(30) NULL ENCODE lzo,
	Lyca_Voucher_card_ID varchar(50) NULL ENCODE lzo,
	PlanId varchar(20) NULL ENCODE lzo,
	Topup_counter varchar(20) NULL ENCODE lzo,
	Bundle_Code varchar(6) NULL ENCODE lzo,
	IMEI varchar(21) NULL ENCODE lzo,
	Voucher_Onnet_Mins_ExpDt varchar(10) NULL ENCODE lzo,
	Voucher_Onnet_Sms_ExpDt varchar(10) NULL ENCODE lzo,
	Voucher_Offnet_Mins_ExpDt1 varchar(10) NULL ENCODE lzo,
	Voucher_Offnet_Sms_ExpDt1 varchar(10) NULL ENCODE lzo,
	Voucher_Offnet_Mins_ExpDt2 varchar(10) NULL ENCODE lzo,
	Voucher_Offnet_Sms_ExpDt2 varchar(10) NULL ENCODE lzo,
	Voucher_Offnet_Mins_ExpDt3 varchar(10) NULL ENCODE lzo,
	Voucher_Offnet_Sms_ExpDt3 varchar(10) NULL ENCODE lzo,
	Voucher_Free_DataExp varchar(10) NULL ENCODE lzo,
	Retailer_Msisdn varchar(21) NULL ENCODE lzo,
	Staff_Msisdn varchar(21) NULL ENCODE lzo,
	Wlm_topupmode varchar(50) NULL ENCODE lzo,
	Face_Value_1 varchar(10) NULL ENCODE lzo,
	Promo_Validity_Days varchar(3) NULL ENCODE lzo,
	OldPlanId varchar(5) NULL ENCODE lzo,
	Onnet_MT_Mins varchar(8) NULL ENCODE lzo,
	Voucher_Onnet_MT_Mins varchar(8) NULL ENCODE lzo,
	Onnet_MT_SMS varchar(8) NULL ENCODE lzo,
	Voucher_Onnet_MT_SMS varchar(8) NULL ENCODE lzo,
	Onnet_MT_EXP_Dt varchar(10) NULL ENCODE lzo,
	Voucher_Onnet_MT_Expiry_date varchar(10) NULL ENCODE lzo,
	Offnet_MT_Mins varchar(8) NULL ENCODE lzo,
	Voucher_Offnet_MT_Mins varchar(8) NULL ENCODE lzo,
	Offnet_MT_SMS varchar(8) NULL ENCODE lzo,
	Voucher_Offnet_MT_SMS varchar(8) NULL ENCODE lzo,
	Offnet_MT_EXP_Dt varchar(10) NULL ENCODE lzo,
	Voucher_Offnet_MT_Expiry_date varchar(10) NULL ENCODE lzo,
	Bundle_name varchar(100) NULL ENCODE lzo,
	Old_Account_Balance varchar(10) NULL ENCODE lzo,
	Parent_MSISDN varchar(21) NULL ENCODE lzo,
	RRBS_Transaction_Id varchar(50) NULL ENCODE lzo,
	Override_Bundle varchar(50) NULL ENCODE lzo,
	VoucherNetworkId varchar(3) NULL ENCODE lzo,
	accid varchar(10) NULL ENCODE lzo,
	Deptid varchar(10) NULL ENCODE lzo,
	Bundle_Catery varchar(50) NULL ENCODE lzo,
	Plan_Validity_Days varchar(50) NULL ENCODE lzo,
	Account_Id varchar(50) NULL ENCODE lzo,
	SIM_Number varchar(50) NULL ENCODE lzo,
	CDR_Time_Stamp varchar(20) NULL ENCODE lzo,
	Reservation varchar(50) NULL ENCODE lzo,
	Discount_promo_code varchar(50) NULL ENCODE lzo,
	Discount_promo_amount varchar(50) NULL ENCODE lzo,
	Primary_bundle_code varchar(50) NULL ENCODE lzo,
	Family_Account_ID varchar(50) NULL ENCODE lzo,
	Tax varchar(50) NULL ENCODE lzo,
	Number_Of_Installments varchar(100) NULL ENCODE lzo,
	OBA_bundle_flag varchar(50) NULL ENCODE lzo,
	OBA_Due_Amount varchar(50) NULL ENCODE lzo,
	Parent_Bundle_Code varchar(50) NULL ENCODE lzo,
	Bundle_Unit_Type varchar(50) NULL ENCODE lzo,
	Contract_Start_Date varchar(50) NULL ENCODE lzo,
	Contract_End_Date varchar(50) NULL ENCODE lzo,
	FLH_Flag varchar(50) NULL ENCODE lzo,
	EXIBS_Retailer_Transaction_ID varchar(50) NULL ENCODE lzo,
	Operation_Flag varchar(50) NULL ENCODE lzo,
	VAT_Amount varchar(50) NULL ENCODE lzo,
	Special_Discount_Code varchar(50) NULL ENCODE lzo,
	Special_Discount_Amount varchar(50) NULL ENCODE lzo,
	Number_Of_installments_Discounts_Applied varchar(50) NULL ENCODE lzo,
	Actual_Bundle_Cost varchar(50) NULL ENCODE lzo,
	Reservation_Reference_Transaction_Id varchar(50) NULL ENCODE lzo,
	Bundle_Expiry_Type varchar(50) NULL ENCODE lzo,
	Bundle_Balance varchar(50) NULL ENCODE lzo,
	MUV_Indicator varchar(50) NULL ENCODE lzo,
	Bundle_Group_Type varchar(50) NULL ENCODE lzo,
	Sequence_Number varchar(50) NULL ENCODE lzo,
	Amount_Refunded varchar(50) NULL ENCODE lzo,
	Forcible_Cancellation varchar(50) NULL ENCODE lzo,
	Payment_Mode_Indicator varchar(50) NULL ENCODE lzo,
	Amount varchar(50) NULL ENCODE lzo,
	Version_ID varchar(50) NULL ENCODE lzo,
	Refund_entity_type varchar(50) NULL ENCODE lzo,
	Refund_provided_to varchar(50) NULL ENCODE lzo,
	Topup_validity_Days varchar(50) NULL ENCODE lzo,
	Reserve_Amount varchar(50) NULL ENCODE lzo,
	Preloaded_Bundle_Flag varchar(50) NULL ENCODE lzo,
	STAFF_INDICATOR varchar(50) NULL ENCODE lzo,
	CHILD_ADDITION_DISCOUNT varchar(50) NULL ENCODE lzo,
	PAYG_FAMILY_INDICATOR varchar(50) NULL ENCODE lzo,
	PAYG_PRORATE_FLAG varchar(50) NULL ENCODE lzo,
	PAYG_CHILD_INDEX varchar(50) NULL ENCODE lzo,
	PARENT_MSISDN1 varchar(50) NULL ENCODE lzo,
	PAYG_MEMBER_COUNT varchar(50) NULL ENCODE lzo,
	DIRECT_ADDON_FLAG varchar(50) NULL ENCODE lzo,
	Action_Flag varchar(50) NULL ENCODE lzo,
	Timestamp varchar(50) NULL ENCODE lzo,
	SIM_with_bundle_Code varchar(50) NULL ENCODE lzo,
	Taxation_Details varchar(50) NULL ENCODE lzo,
	reserved varchar(50) NULL ENCODE lzo,
	Bundle_Purchase_Date varchar(50) NULL ENCODE lzo,
	Bundle_Start_Date varchar(50) NULL ENCODE lzo,
	Renewal_Payment_Mode varchar(50) NULL ENCODE lzo,
	IOT_Bundle_flag varchar(50) NULL ENCODE lzo,
	VeekBundleFlag varchar(50) NULL ENCODE lzo,
	validity_overridden varchar(50) NULL ENCODE lzo,
	link_type varchar(50) NULL ENCODE lzo,
	overwrite_bundle_cost_flag varchar(50) NULL ENCODE lzo,
	Order_id varchar(50) NULL ENCODE lzo,
	Pocket_id varchar(50) NULL ENCODE lzo,
	DA_Track varchar(50) NULL ENCODE lzo,
	DA_Principal_Amount varchar(50) NULL ENCODE lzo,
	Is_Loan_Request varchar(50) NULL ENCODE lzo,
	Migrated_Ortel_Subscriber_Flag varchar(50) NULL ENCODE lzo,
	Reward_Purchased varchar(50) NULL ENCODE lzo,
	feebuffer varchar(50) NULL ENCODE lzo,
	Tax_Buffer varchar(50) NULL ENCODE lzo,
	PaymentGateway varchar(50) NULL ENCODE lzo,
	Zipcode varchar(50) NULL ENCODE lzo,
	Professional_Channel_Name varchar(50) NULL ENCODE lzo,
	TopupCriteriaFlag varchar(50) NULL ENCODE lzo,
	NUS_ID varchar(50) NULL ENCODE lzo,
	NUS_APPLIED varchar(50) NULL ENCODE lzo,
	SlotId varchar(50) NULL ENCODE lzo,
	Reserved_Bundle_Cost varchar(50) NULL ENCODE lzo
);

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
