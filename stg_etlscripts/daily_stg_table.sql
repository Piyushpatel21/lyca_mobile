---###Creates daily stg table --------------------------------------------------
CREATE TABLE ukdev.stg.temp_stg_VOICE_2019110723(
	Voice_call_cdr int NULL,
	Network_ID varchar(10) NULL,
	Call_Type varchar(10) NULL,
	call_feature varchar(10) NULL,
	TariffPlan_ID int NULL,
	Service_ID int NULL,
	cli varchar(20) NULL,
	Dialed_number varchar(50) NULL,
	Charged_party_number varchar(20) NULL,
	IMSI varchar(50) NULL,
	Serving_Node varchar(30) NULL,
	MCC varchar(50) NULL,
	MNC varchar(50) NULL,
	LAC varchar(50) NULL,
	Calling_Cell_ID varchar(30) NULL,
	CellZone_Code varchar(30) NULL,
	CellZone_Name varchar(30) NULL,
	Original_Dialed_number varchar(30) NULL,
	Destination_zone varchar(30) NULL,
	Destination_area_code varchar(30) NULL,
	Destinationzone_name varchar(30) NULL,
	Roaming_area_code varchar(20) NULL,
	Roaming_zone_name varchar(30) NULL,
	roam_flag varchar(10) NOT NULL,
	Roam_area_number varchar(10) NULL,
	Granted_Time varchar(10) NULL,
	Granted_Money decimal(10, 4) NULL,
	call_duration varchar(10) NULL,
	Chargeable_Used_Time varchar(30) NULL,
	Call_date varchar(30) NULL,
	Call_termination_time varchar(30) NULL,
	Initial_Account_balance decimal(10, 4) NULL,
	Talk_charge decimal(10, 4) NULL,
	balance decimal(10, 4) NULL,
	Free_minutes_account_balance decimal(20, 6) NULL,
	Account_Id int NULL,
	Dept_Id int NULL,
	Free_Zone_Id varchar(50) NULL,
	Trace_Id varchar(50) NULL,
	Last_Topup_Type varchar(1) NULL,
	Bundle_Code varchar(10) NULL,
	Free_Zone_Expiry_Date varchar(15) NULL,
	Ringing_Duration varchar(3) NULL,
	Ring_Indicator varchar(1) NULL,
	National_Bundle_Code varchar(10) NULL,
	National_Used_Minutes varchar(10) NULL,
	National_Charge varchar(10) NULL,
	Pool_Number varchar(20) NULL,
	Bundle_used_seconds varchar(10) NULL,
	Bundle_Call_Charge varchar(10) NULL,
	Bundle_Balance varchar(10) NULL,
	Bundle_Plan_ID varchar(5) NULL,
	Final_Unit_Indicator varchar(1) NULL,
	NCALL_Free_Units varchar(10) NULL,
	Local_Roam_Country_Code varchar(4) NULL,
	CDR_Time_Stamp varchar(50) NULL,
	Bucket_type varchar(50) NULL,
	Conversion_Unit varchar(50) NULL,
	Intial_Free_units varchar(50) NULL,
	Subscriber_Type varchar(50) NULL,
	Call_Forwarding_Indicator varchar(50) NULL,
	CDR_Sequence_Number varchar(50) NULL,
	Sub_Acct_Id varchar(50) NULL,
	Announcement_Time varchar(50) NULL,
	Family_id varchar(50) NULL,
	Idp_Time varchar(100) NULL,
	CS_free_Mins varchar(50) NULL,
	Free_Units_Detected varchar(50) NULL,
	Multileg_Charging_Flag varchar(50) NULL,
	Extension_Record varchar(50) NULL,
	Connect_Number varchar(50) NULL,
	Dialed_Number1 varchar(50) NULL,
	Service_Description varchar(100) NULL,
	Alternate_Plan_Id varchar(50) NULL,
	Serving_Node_Name varchar(50) NULL,
	Fixed_charge varchar(50) NULL,
	Fixed_charge_Resource_Impact varchar(50) NULL,
	Bundle_Version_Name varchar(50) NULL,
	FLH_Reward_slab_id varchar(50) NULL,
	Bundle_version_ID varchar(50) NULL,
	Rate_ID varchar(50) NULL,
	Pricing_Plan_ID varchar(50) NULL,
	Pocket_Id varchar(50) NULL,
	Consumed_Promo_Amount varchar(50) NULL,
	Pervious_call_trace_Id varchar(50) NULL,
	Termination_Cause varchar(50) NULL,
	Non_Chargeable_Unit varchar(50) NULL,
	Call_Forward_MSISDN varchar(50) NULL,
	Service_Type varchar(50) NULL,
	Used_time_in_previous_grant varchar(50) NULL,
	RLH_Charging_Indicator varchar(50) NULL,
	Surcharge_Consumed varchar(50) NULL,
	Excess_Used_units_received varchar(50) NULL,
	Failcause varchar(50) NULL,
	Roaming_Partner_ID varchar(50) NULL
)

---###ingest the data from S3 to table --------------------------------------------------
copy ukdev.stg.temp_stg_VOICE_2019110723 from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/RRBS/UK/VOICE/2019/11/07/VOICE_2019110723.cdr'
iam_role 'arn:aws:iam::311477489434:role/Redshift-S3-uk-AllSubAccounts'
csv
delimiter ',';

---###add new coloumns for filename and creation date --------------------------------------------------
ALTER TABLE ukdev.stg.temp_stg_VOICE_2019110723 ADD COLUMN FileName varchar(50) NOT NULL DEFAULT 'VOICE_2019110723.cdr';
ALTER TABLE ukdev.stg.temp_stg_VOICE_2019110723 ADD COLUMN Created_Date datetime default sysdate;

