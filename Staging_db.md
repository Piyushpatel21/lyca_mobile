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



#### RRBS Voice table :"stag_ukdev_uk_rrbs_gprs"

```sql
CREATE TABLE staging.stag_ukdev_uk_rrbs_gprs(
	[RRBS_GID] [bigint] IDENTITY(1,1) NOT NULL,
	[CDR_Type] [int] NULL,
	[Network_ID] [int] NULL,
	[Data_Feature] [int] NULL,
	[TariffPlan_ID] [int] NULL,
	[Service_ID] [int] NULL,
	[Msisdn] [varchar](20) NULL,
	[APN] [varchar](100) NULL,
	[PDP_Address] [varchar](50) NULL,
	[cellId] [varchar](50) NULL,
	[IMEI] [varchar](50) NULL,
	[IMSI] [varchar](20) NULL,
	[Serving_Node] [varchar](30) NULL,
	[GGSN_Address] [varchar](50) NULL,
	[Roaming_zone_name] [varchar](30) NULL,
	[Roam_Flag] [int] NULL,
	[Granted_bytes_cumulative] [bigint] NULL,
	[Granted_Money] [decimal](20, 6) NULL,
	[Total_Used_Bytes] [bigint] NULL,
	[Chargeable_Used_Bytes] [bigint] NULL,
	[Uploaded_Bytes] [int] NULL,
	[Downloaded_Bytes] [int] NULL,
	[Data_connection_time] [varchar](20) NULL,
	[Data_termination_time] [varchar](20) NULL,
	[Time_Duration] [varchar](20) NULL,
	[Initial_Account_balance] [decimal](20, 6) NULL,
	[Data_charge] [decimal](20, 6) NULL,
	[Final_Account_balance] [decimal](20, 6) NULL,
	[Free_Bytes] [varchar](50) NULL,
	[Sessionid] [varchar](50) NULL,
	[Last_Topup_Type] [varchar](50) NULL,
	[Free_Data_Expiry_Date] [varchar](10) NULL,
	[Charge_Indicator] [varchar](50) NULL,
	[Final_unit_indicator] [varchar](50) NULL,
	[Bundle_Code] [varchar](10) NULL,
	[AccId] [varchar](11) NULL,
	[DeptId] [varchar](15) NULL,
	[Bundle_used_data] [varchar](10) NULL,
	[Bundle_data_Charge] [varchar](10) NULL,
	[Bundle_balance] [varchar](10) NULL,
	[Bundle_Plan_ID] [varchar](5) NULL,
	[Local_Roam_Country_Code] [varchar](4) NULL,
	[sdfId] [varchar](100) NULL,
	[RateGroupID] [varchar](4) NULL,
	[ServiceID] [varchar](4) NULL,
	[CDR_Time_Stamp] [varchar](20) NULL,
	[Intial_Free_units] [varchar](50) NULL,
	[Subscriber_Type] [varchar](50) NULL,
	[CDR_Sequence_Number] [varchar](50) NULL,
	[Sub_Acct_Id] [varchar](50) NULL,
	[GGSN_Time] [varchar](100) NULL,
	[Tariff_Plan_Change_ID] [varchar](50) NULL,
	[Used_Units_Before_Tariff_Plan_Change] [varchar](50) NULL,
	[Family_Account_ID] [varchar](50) NULL,
	[Bundle_Version_Name] [varchar](50) NULL,
	[Bundle_VersionId] [varchar](50) NULL,
	[Reward_slab_Id] [varchar](50) NULL,
	[Pricing_Plan_ID] [varchar](50) NULL,
	[Rate_ID] [varchar](50) NULL,
	[Pocket_Id] [varchar](50) NULL,
	[Consumed_Promo_Amount] [varchar](50) NULL,
	[Consumed_Surcharge] [varchar](50) NULL,
	[RLH_Charging_Indicator] [varchar](50) NULL,
	[Granted_bytes_current] [varchar](50) NULL,
	[Roaming_Partner_ID] [varchar](50) NULL,
	[FileName] [varchar](40) NULL,
	[CREATED_DATE] [datetime] NULL
) ON [PRIMARY]

GO

CREATE NONCLUSTERED INDEX [ind_dtc] ON staging.stag_ukdev_uk_rrbs_gprs 
(
	[Data_connection_time] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON, FILLFACTOR = 80) ON [PRIMARY]
GO


CREATE NONCLUSTERED INDEX [ind_MSISDN] ON staging.stag_ukdev_uk_rrbs_gprs 
(
	[Msisdn] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON, FILLFACTOR = 80) ON [PRIMARY]
GO

```



#### RRBS Voice table :"stag_ukdev_uk_rrbs_sms"

```sql
CREATE TABLE staging.stag_ukdev_uk_rrbs_sms(
	[RRBS_SID] [bigint] IDENTITY(1,1) NOT NULL,
	[CDR_Types] [varchar](40) NULL,
	[Network_ID] [varchar](10) NULL,
	[Call_Type] [varchar](5) NULL,
	[Plan_Id] [int] NULL,
	[Service_Id] [int] NULL,
	[cli] [varchar](20) NULL,
	[Dialed_number] [varchar](50) NULL,
	[IMSI] [varchar](30) NULL,
	[Serving_Node] [varchar](30) NULL,
	[Destination_area_code] [varchar](50) NULL,
	[Destination_zone_code] [varchar](50) NULL,
	[Destination_zone_name] [varchar](50) NULL,
	[Roam_Flag] [varchar](50) NULL,
	[Roaming_node] [varchar](50) NULL,
	[Roaming_area_code] [varchar](50) NULL,
	[Roaming_zone_name] [varchar](50) NULL,
	[SMS_Feature] [varchar](50) NULL,
	[Number_of_SMS_charged] [varchar](10) NULL,
	[Number_of_free_SMS] [varchar](20) NULL,
	[Initial_Account_balance] [decimal](10, 4) NULL,
	[msg_cost] [decimal](10, 4) NULL,
	[balance] [decimal](10, 4) NULL,
	[Free_SMS_account_balance] [varchar](100) NULL,
	[Instance_ID_Session_ID] [varchar](20) NULL,
	[msg_date] [varchar](21) NOT NULL,
	[Account_Id] [int] NULL,
	[Dept_Id] [int] NULL,
	[Free_Zone_Id] [varchar](10) NULL,
	[Last_Topup_Type] [varchar](1) NULL,
	[Bundle_Code] [varchar](6) NULL,
	[Free_Zone_Expiry_Date] [varchar](10) NULL,
	[Bundle_SMS_Charge] [varchar](10) NULL,
	[Bundle_Balance] [varchar](10) NULL,
	[Bundle_Plan_ID] [varchar](5) NULL,
	[NSMS_Free_Units] [varchar](10) NULL,
	[Local_Roam_Country_Code] [varchar](4) NULL,
	[CDR_Time_Stamp] [varchar](21) NULL,
	[Bucket_type] [varchar](50) NULL,
	[Intial_Free_units] [varchar](50) NULL,
	[Subscriber_Type] [varchar](50) NULL,
	[Sub_Acct_Id] [varchar](50) NULL,
	[Family_id] [varchar](50) NULL,
	[Concat_Message_Flag] [varchar](50) NULL,
	[Concat_Message_Reference_Number] [varchar](50) NULL,
	[Concat_Message_Total_chunks] [varchar](50) NULL,
	[Concat_Message_Current_chunk] [varchar](50) NULL,
	[Trace_Id] [varchar](50) NULL,
	[Idp_Time] [varchar](50) NULL,
	[Extension_Record] [varchar](50) NULL,
	[Call_Indicator] [varchar](50) NULL,
	[Serving_Node_Name] [varchar](50) NULL,
	[Bundle_Version_Name] [varchar](50) NULL,
	[FLH_Reward_slab_id] [varchar](50) NULL,
	[Bundle_version_ID] [varchar](50) NULL,
	[Rate_ID] [varchar](50) NULL,
	[Pricing_Plan_ID] [varchar](50) NULL,
	[Pocket_Id] [varchar](50) NULL,
	[Consumed_Promo_Amount] [varchar](50) NULL,
	[RLH_Charging_Indicator] [varchar](50) NULL,
	[Surcharge_Consumed] [varchar](50) NULL,
	[Roaming_Partner_ID] [varchar](50) NULL,
	[FileName] [varchar](100) NULL,
	[Created_date] [datetime] NULL
) ON [PRIMARY]

CREATE NONCLUSTERED INDEX [idx_dn] ON staging.stag_ukdev_uk_rrbs_sms 
(
	[Dialed_number] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON, FILLFACTOR = 80) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [ind_cli7] ON staging.stag_ukdev_uk_rrbs_sms 
(
	[cli] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON, FILLFACTOR = 80) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [ind_date7] ON staging.stag_ukdev_uk_rrbs_sms 
(
	[msg_date] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON, FILLFACTOR = 80) ON [PRIMARY]
GO

```



#### RRBS Voice table :"stag_ukdev_uk_rrbs_topup"

```sql
CREATE TABLE staging.stag_ukdev_uk_rrbs_topup(
	[RRBS_TID] [bigint] IDENTITY(1,1) NOT NULL,
	[Operation_Code] [varchar](10) NULL,
	[Network_ID] [varchar](50) NULL,
	[recharge_type] [varchar](10) NOT NULL,
	[MSISDN] [varchar](50) NULL,
	[IMSI] [varchar](30) NULL,
	[account_pin_number] [varchar](50) NULL,
	[Voucher_card_ID] [varchar](50) NULL,
	[Special_topup_amount] [decimal](10, 4) NULL,
	[Transaction_ID] [varchar](50) NULL,
	[new_balance] [decimal](10, 4) NULL,
	[Face_value] [decimal](10, 4) NULL,
	[rechage_amount] [decimal](10, 4) NULL,
	[Promo_Validity_Date] [varchar](50) NULL,
	[Free_minutes] [varchar](20) NULL,
	[Free_Voucher_Onnet_Minutes] [varchar](20) NULL,
	[Free_minutes_expiry_date] [varchar](20) NULL,
	[Free_SMS] [varchar](30) NULL,
	[Free_Voucher_Onnet_SMS] [varchar](20) NULL,
	[Free_SMS_expiry_date] [varchar](20) NULL,
	[Free_Offnet_Minutes] [varchar](20) NULL,
	[Free_Voucher_Offnet_Minutes] [varchar](20) NULL,
	[Free_Offnet_Minutes_Expiry_Date] [varchar](20) NULL,
	[Free_Offnet_SMS] [varchar](20) NULL,
	[Free_Voucher_Offnet_SMS] [varchar](20) NULL,
	[Free_Offnet_SMS_Expiry_Date] [varchar](20) NULL,
	[Free_OffNet2_Minutes] [varchar](20) NULL,
	[Free_VoucherCard_Offnet2_Minutes] [varchar](20) NULL,
	[Free_OffNet2_Minutes_expiry_date] [varchar](20) NULL,
	[Free_OffNet2_SMS] [varchar](20) NULL,
	[Free_VoucherCard_Offnet2_SMS] [varchar](20) NULL,
	[Free_OffNet2_SMS_expiry_date] [varchar](20) NULL,
	[Free_OffNet3_Minutes] [varchar](20) NULL,
	[Free_VoucherCard_Offnet3_Minutes] [varchar](20) NULL,
	[Free_OffNet3_Minutes_expiry_date] [varchar](20) NULL,
	[Free_OffNet3_SMS] [varchar](20) NULL,
	[Free_VoucherCard_Offnet3_SMS] [varchar](20) NULL,
	[Free_OffNet3_SMS_expiry_date] [varchar](20) NULL,
	[Free_Data] [varchar](20) NULL,
	[Free_Voucher_Data] [varchar](20) NULL,
	[Free_Data_ExpiryDate] [varchar](20) NULL,
	[Account_validity_date] [varchar](30) NULL,
	[Lyca_Voucher_card_ID] [varchar](50) NULL,
	[PlanId] [varchar](20) NULL,
	[Topup_counter] [varchar](20) NULL,
	[Bundle_Code] [varchar](6) NULL,
	[IMEI] [varchar](21) NULL,
	[Voucher_Onnet_Mins_ExpDt] [varchar](10) NULL,
	[Voucher_Onnet_Sms_ExpDt] [varchar](10) NULL,
	[Voucher_Offnet_Mins_ExpDt1] [varchar](10) NULL,
	[Voucher_Offnet_Sms_ExpDt1] [varchar](10) NULL,
	[Voucher_Offnet_Mins_ExpDt2] [varchar](10) NULL,
	[Voucher_Offnet_Sms_ExpDt2] [varchar](10) NULL,
	[Voucher_Offnet_Mins_ExpDt3] [varchar](10) NULL,
	[Voucher_Offnet_Sms_ExpDt3] [varchar](10) NULL,
	[Voucher_Free_DataExp] [varchar](10) NULL,
	[Retailer_Msisdn] [varchar](21) NULL,
	[Staff_Msisdn] [varchar](21) NULL,
	[Wlm_topupmode] [varchar](50) NULL,
	[Face_Value_1] [varchar](10) NULL,
	[Promo_Validity_Days] [varchar](3) NULL,
	[OldPlanId] [varchar](5) NULL,
	[Onnet_MT_Mins] [varchar](8) NULL,
	[Voucher_Onnet_MT_Mins] [varchar](8) NULL,
	[Onnet_MT_SMS] [varchar](8) NULL,
	[Voucher_Onnet_MT_SMS] [varchar](8) NULL,
	[Onnet_MT_EXP_Dt] [varchar](10) NULL,
	[Voucher_Onnet_MT_Expiry_date] [varchar](10) NULL,
	[Offnet_MT_Mins] [varchar](8) NULL,
	[Voucher_Offnet_MT_Mins] [varchar](8) NULL,
	[Offnet_MT_SMS] [varchar](8) NULL,
	[Voucher_Offnet_MT_SMS] [varchar](8) NULL,
	[Offnet_MT_EXP_Dt] [varchar](10) NULL,
	[Voucher_Offnet_MT_Expiry_date] [varchar](10) NULL,
	[Bundle_name] [varchar](100) NULL,
	[Old_Account_Balance] [varchar](10) NULL,
	[Parent_MSISDN] [varchar](21) NULL,
	[RRBS_Transaction_Id] [varchar](50) NULL,
	[Override_Bundle] [varchar](50) NULL,
	[VoucherNetworkId] [varchar](3) NULL,
	[accid] [varchar](10) NULL,
	[Deptid] [varchar](10) NULL,
	[Bundle_Category] [varchar](50) NULL,
	[Plan_Validity_Days] [varchar](50) NULL,
	[Account_Id] [varchar](50) NULL,
	[SIM_Number] [varchar](50) NULL,
	[CDR_Time_Stamp] [varchar](20) NULL,
	[Reservation] [varchar](50) NULL,
	[Discount_promo_code] [varchar](50) NULL,
	[Discount_promo_amount] [varchar](50) NULL,
	[Primary_bundle_code] [varchar](50) NULL,
	[Family_Account_ID] [varchar](50) NULL,
	[Tax] [varchar](50) NULL,
	[Number_Of_Installments] [varchar](100) NULL,
	[OBA_bundle_flag] [varchar](50) NULL,
	[OBA_Due_Amount] [varchar](50) NULL,
	[Parent_Bundle_Code] [varchar](50) NULL,
	[Bundle_Unit_Type] [varchar](50) NULL,
	[Contract_Start_Date] [varchar](50) NULL,
	[Contract_End_Date] [varchar](50) NULL,
	[FLH_Flag] [varchar](50) NULL,
	[EXIBS_Retailer_Transaction_ID] [varchar](50) NULL,
	[Operation_Flag] [varchar](50) NULL,
	[VAT_Amount] [varchar](50) NULL,
	[Special_Discount_Code] [varchar](50) NULL,
	[Special_Discount_Amount] [varchar](50) NULL,
	[Number_Of_installments_Discounts_Applied] [varchar](50) NULL,
	[Actual_Bundle_Cost] [varchar](50) NULL,
	[Reservation_Reference_Transaction_Id] [varchar](50) NULL,
	[Bundle_Expiry_Type] [varchar](50) NULL,
	[Bundle_Balance] [varchar](50) NULL,
	[MUV_Indicator] [varchar](50) NULL,
	[Bundle_Group_Type] [varchar](50) NULL,
	[Sequence_Number] [varchar](50) NULL,
	[Amount_Refunded] [varchar](50) NULL,
	[Forcible_Cancellation] [varchar](50) NULL,
	[Payment_Mode_Indicator] [varchar](50) NULL,
	[Amount] [varchar](50) NULL,
	[Version_ID] [varchar](50) NULL,
	[Refund_entity_type] [varchar](50) NULL,
	[Refund_provided_to] [varchar](50) NULL,
	[Topup_validity_Days] [varchar](50) NULL,
	[Reserve_Amount] [varchar](50) NULL,
	[Preloaded_Bundle_Flag] [varchar](50) NULL,
	[STAFF_INDICATOR] [varchar](50) NULL,
	[CHILD_ADDITION_DISCOUNT] [varchar](50) NULL,
	[PAYG_FAMILY_INDICATOR] [varchar](50) NULL,
	[PAYG_PRORATE_FLAG] [varchar](50) NULL,
	[PAYG_CHILD_INDEX] [varchar](50) NULL,
	[PARENT_MSISDN1] [varchar](50) NULL,
	[PAYG_MEMBER_COUNT] [varchar](50) NULL,
	[DIRECT_ADDON_FLAG] [varchar](50) NULL,
	[Action_Flag] [varchar](50) NULL,
	[Timestamp] [varchar](50) NULL,
	[SIM_with_bundle_Code] [varchar](50) NULL,
	[Taxation_Details] [varchar](50) NULL,
	[reserved] [varchar](50) NULL,
	[Bundle_Purchase_Date] [varchar](50) NULL,
	[Bundle_Start_Date] [varchar](50) NULL,
	[Renewal_Payment_Mode] [varchar](50) NULL,
	[IOT_Bundle_flag] [varchar](50) NULL,
	[VeekBundleFlag] [varchar](50) NULL,
	[validity_overridden] [varchar](50) NULL,
	[link_type] [varchar](50) NULL,
	[overwrite_bundle_cost_flag] [varchar](50) NULL,
	[Order_id] [varchar](50) NULL,
	[Pocket_id] [varchar](50) NULL,
	[DA_Track] [varchar](50) NULL,
	[DA_Principal_Amount] [varchar](50) NULL,
	[Is_Loan_Request] [varchar](50) NULL,
	[Migrated_Ortel_Subscriber_Flag] [varchar](50) NULL,
	[Reward_Purchased] [varchar](50) NULL,
	[feebuffer] [varchar](50) NULL,
	[Tax_Buffer] [varchar](50) NULL,
	[PaymentGateway] [varchar](50) NULL,
	[Zipcode] [varchar](50) NULL,
	[Professional_Channel_Name] [varchar](50) NULL,
	[TopupCriteriaFlag] [varchar](50) NULL,
	[NUS_ID] [varchar](50) NULL,
	[NUS_APPLIED] [varchar](50) NULL,
	[SlotId] [varchar](50) NULL,
	[Reserved_Bundle_Cost] [varchar](50) NULL,
	[Additional_Channel_Info] [varchar](50) NULL,
	[AutoFlag] [varchar](50) NULL,
	[BundleExpiryDate] [varchar](50) NULL,
	[recharge_date] [varchar](22) NOT NULL,
	[Filename] [varchar](50) NULL,
	[Created_date] [datetime] NULL
) ON [PRIMARY]

GO


CREATE NONCLUSTERED INDEX [ind_date7] ON staging.stag_ukdev_uk_rrbs_topup 
(
	[recharge_date] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON, FILLFACTOR = 80) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [ind_MSISDN] ON staging.stag_ukdev_uk_rrbs_topup 
(
	[MSISDN] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON, FILLFACTOR = 80) ON [PRIMARY]
GO

```


