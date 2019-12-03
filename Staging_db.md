#Redshift Staging db 

This document contains the documentation of the staging db and creating + loading data to tables

## Commands to create tables

### Staging tables for RRBS source systems

#### RRBS Voice table :"stag_ukdev_uk_rrbs_voice"

```sql
CREATE TABLE staging.stag_ukdev_uk_rrbs_voice(
	[RRBS_VID] [bigint] IDENTITY(1,1) NOT NULL,
	[Voice_call_cdr] [int] NULL,
	[Network_ID] [varchar](10) NULL,
	[Call_Type] [varchar](10) NULL,
	[call_feature] [varchar](10) NULL,
	[TariffPlan_ID] [int] NULL,
	[Service_ID] [int] NULL,
	[cli] [varchar](20) NULL,
	[Dialed_number] [varchar](50) NULL,
	[Charged_party_number] [varchar](20) NULL,
	[IMSI] [varchar](50) NULL,
	[Serving_Node] [varchar](30) NULL,
	[MCC] [varchar](50) NULL,
	[MNC] [varchar](50) NULL,
	[LAC] [varchar](50) NULL,
	[Calling_Cell_ID] [varchar](30) NULL,
	[CellZone_Code] [varchar](30) NULL,
	[CellZone_Name] [varchar](30) NULL,
	[Original_Dialed_number] [varchar](30) NULL,
	[Destination_zone] [varchar](30) NULL,
	[Destination_area_code] [varchar](30) NULL,
	[Destinationzone_name] [varchar](30) NULL,
	[Roaming_area_code] [varchar](20) NULL,
	[Roaming_zone_name] [varchar](30) NULL,
	[roam_flag] [varchar](10) NOT NULL,
	[Roam_area_number] [varchar](10) NULL,
	[Granted_Time] [varchar](10) NULL,
	[Granted_Money] [decimal](10, 4) NULL,
	[call_duration] [varchar](10) NULL,
	[Chargeable_Used_Time] [varchar](30) NULL,
	[Call_date] [varchar](30) NULL,
	[Call_termination_time] [varchar](30) NULL,
	[Initial_Account_balance] [decimal](10, 4) NULL,
	[Talk_charge] [decimal](10, 4) NULL,
	[balance] [decimal](10, 4) NULL,
	[Free_minutes_account_balance] [decimal](20, 6) NULL,
	[Account_Id] [int] NULL,
	[Dept_Id] [int] NULL,
	[Free_Zone_Id] [varchar](50) NULL,
	[Trace_Id] [varchar](50) NULL,
	[Last_Topup_Type] [varchar](1) NULL,
	[Bundle_Code] [varchar](10) NULL,
	[Free_Zone_Expiry_Date] [varchar](15) NULL,
	[Ringing_Duration] [varchar](3) NULL,
	[Ring_Indicator] [varchar](1) NULL,
	[National_Bundle_Code] [varchar](10) NULL,
	[National_Used_Minutes] [varchar](10) NULL,
	[National_Charge] [varchar](10) NULL,
	[Pool_Number] [varchar](20) NULL,
	[Bundle_used_seconds] [varchar](10) NULL,
	[Bundle_Call_Charge] [varchar](10) NULL,
	[Bundle_Balance] [varchar](10) NULL,
	[Bundle_Plan_ID] [varchar](5) NULL,
	[Final_Unit_Indicator] [varchar](1) NULL,
	[NCALL_Free_Units] [varchar](10) NULL,
	[Local_Roam_Country_Code] [varchar](4) NULL,
	[CDR_Time_Stamp] [varchar](50) NULL,
	[Bucket_type] [varchar](50) NULL,
	[Conversion_Unit] [varchar](50) NULL,
	[Intial_Free_units] [varchar](50) NULL,
	[Subscriber_Type] [varchar](50) NULL,
	[Call_Forwarding_Indicator] [varchar](50) NULL,
	[CDR_Sequence_Number] [varchar](50) NULL,
	[Sub_Acct_Id] [varchar](50) NULL,
	[Announcement_Time] [varchar](50) NULL,
	[Family_id] [varchar](50) NULL,
	[Idp_Time] [varchar](100) NULL,
	[CS_free_Mins] [varchar](50) NULL,
	[Free_Units_Detected] [varchar](50) NULL,
	[Multileg_Charging_Flag] [varchar](50) NULL,
	[Extension_Record] [varchar](50) NULL,
	[Connect_Number] [varchar](50) NULL,
	[Dialed_Number1] [varchar](50) NULL,
	[Service_Description] [varchar](100) NULL,
	[Alternate_Plan_Id] [varchar](50) NULL,
	[Serving_Node_Name] [varchar](50) NULL,
	[Fixed_charge] [varchar](50) NULL,
	[Fixed_charge_Resource_Impact] [varchar](50) NULL,
	[Bundle_Version_Name] [varchar](50) NULL,
	[FLH_Reward_slab_id] [varchar](50) NULL,
	[Bundle_version_ID] [varchar](50) NULL,
	[Rate_ID] [varchar](50) NULL,
	[Pricing_Plan_ID] [varchar](50) NULL,
	[Pocket_Id] [varchar](50) NULL,
	[Consumed_Promo_Amount] [varchar](50) NULL,
	[Pervious_call_trace_Id] [varchar](50) NULL,
	[Termination_Cause] [varchar](50) NULL,
	[Non_Chargeable_Unit] [varchar](50) NULL,
	[Call_Forward_MSISDN] [varchar](50) NULL,
	[Service_Type] [varchar](50) NULL,
	[Used_time_in_previous_grant] [varchar](50) NULL,
	[RLH_Charging_Indicator] [varchar](50) NULL,
	[Surcharge_Consumed] [varchar](50) NULL,
	[Excess_Used_units_received] [varchar](50) NULL,
	[Failcause] [varchar](50) NULL,
	[Roaming_Partner_ID] [varchar](50) NULL,
	[FileName] [varchar](100) NULL,
	[CREATED_DATE] [datetime] NULL
) ON [PRIMARY]

USE [GBR_MONTH10_2019]
GO

CREATE NONCLUSTERED INDEX [idx_cpn] ON staging.stag_ukdev_uk_rrbs_cdr 
(
	[Charged_party_number] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON, FILLFACTOR = 80) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [idx_CT] ON staging.stag_ukdev_uk_rrbs_cdr 
(
	[Call_Type] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON, FILLFACTOR = 80) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [idx_dn] ON staging.stag_ukdev_uk_rrbs_cdr 
(
	[Dialed_number] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON, FILLFACTOR = 80) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [ind_cli7] ON staging.stag_ukdev_uk_rrbs_cdr 
(
	[cli] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON, FILLFACTOR = 80) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [ind_date7] ON staging.stag_ukdev_uk_rrbs_cdr 
(
	[Call_date] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON, FILLFACTOR = 80) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [ind_IMSI] ON staging.stag_ukdev_uk_rrbs_cdr 
(
	[IMSI] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON, FILLFACTOR = 80) ON [PRIMARY]
GO
```

