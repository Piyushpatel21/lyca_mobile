select 
tab_user_count.count_date as date_usertab
,tab_activation.activations
, tab_user_count.dailyUserCount
,tab_all_voice.*
,tab_all_topup.*
,tab_all_sms.*
,tab_data_user.*
,dd.*
from 
(


----------------------------daily user count-------------------------------------------------------------------

select
	count_date, count(cli) as dailyUserCount
from
	(
	select
		distinct call_date_dt as count_date, Charged_party_number as cli
		--into    #daily                

		from uk_rrbs_dm.fact_rrbs_voice
		

		where 
		 network_id = 1
		and dialed_number not in ('447404000130',		'7404000130')		
		and call_duration > 0
		
union
	select
		distinct msg_date_dt  as count_date, cli
	from
		uk_rrbs_dm.fact_rrbs_sms
		--Sms_UK_daily   (nolock)              

		where 
		 network_id = 1
		and cdr_types = 50
		
union
	select
		distinct data_termination_dt  as count_date, msisdn as cli
	from
		uk_rrbs_dm.fact_rrbs_gprs
		              

		where 
		 network_id = 1
		and Total_Used_Bytes > 0
		--and left(data_termination_time,8)= CONVERT(varchar(8),GETDATE()-1,112) 
		and ( RateGroupID <> '1000' or RateGroupId is null) 
		--group by data_termination_dt
		) X
		group by count_date
		order by count_date
		
		) as tab_user_count (count_date, dailyUserCount)
join

(
-----------------------------------Voice Combined------------------------<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

select call_date_dt
, (int_min + nat_min + moc_roam_outgoing) as total_mins
,onnet_min
,natx_net_min
,moc_roam_outgoing
,nat_min
,int_min
,nat_fix
,global_onnet_mins
from (

select call_date_dt
, sum(case when roam_flag in(0,1) and Call_TYPE =7 and     dialed_number not in ('447404000321','44321','447404000121','44121','447404000130','447404000322','44322')  then call_duration else 0 end )/60 ::decimal(20,4) as  INT_MIN
, sum(case when roam_flag in(0,1) and Call_TYPE in(1,3,5)  and dialed_number not in ('447404000321','44321','447404000121','44121','447404000130','447404000322','44322')   then call_duration else 0 end)/60 ::decimal(20,4) as nat_min
, sum(case when roam_flag =2    and Call_TYPE in( 1,3,5,7)   then call_duration else 0 end )/60 ::decimal(20,4) as moc_roam_outgoing

,sum(case when roam_flag  in (0,1) and call_type =1 and dialed_number not in ('447404000321','44321','447404000121','44121','447404000130','447404000322','44322')  then call_duration  else 0 end)/60 ::decimal(20,4) as onnet_min

,sum(case when roam_flag  in (0,1) and Call_TYPE in (3,5) and dialed_number not in ('447404000321','44321','447404000121','44121','447404000130','447404000322','44322')  then (call_duration)else 0 end)/60 ::decimal(20,4) as Natx_net_min
,sum(case when roam_flag  in (0,1) and Call_TYPE = 5 then (call_duration) else 0 end) /60 ::decimal(20,4) as nat_fix
--,sum(case when roam_flag  in (0,1) and Call_TYPE =7  and dialed_number not in ('447404000321','44321','447404000121','44121','447404000130','447404000322','44322')   then (call_duration) else 0 end)/60 ::decimal(20,4) as Int_min
,sum(case when Call_TYPE = 7 and call_feature = 13 then call_duration else 0 end )/60 ::decimal(20,4) as global_onnet_mins

from 
	uk_rrbs_dm.fact_rrbs_voice frv
where
--			call_date_dt ='2019-09-15' -- >= '2019-09-01' and call_date_dt <= '2019-09-30'
	 	network_id = 1
	and 	call_duration > 0
	group by call_date_dt
) X	
	order by call_date_dt
	) as tab_all_voice(call_date_dt, total_mins, onnet_min,natx_net_min,moc_roam_outgoing,nat_min, int_min,nat_fix,global_onnet_mins)
	
	on tab_user_count.count_date = tab_all_voice.call_date_dt
	
	join (
-----------------------------TopUp--------------<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
select cdr_dt
, sum( case when Bundle_Name is null			  	
			and operation_code in (3,1,10) 
			and (face_value+special_topup_amount)>0
			then 1 else 0 end ) as Voucher_Count
, sum(case when operation_code in (5,12,14)
			and (actual_bundle_cost) > 0 
			then 1 else 0 end )	as bundle_count		
FROM uk_rrbs_dm.fact_rrbs_topup frt
where
--cdr_dt = '2019-09-15' and  
network_id = '1'
and bundle_code not in ('111333','111334','111335','111336','111337','111338','111339','111340','111341','111342','447729')
group by cdr_dt
) tab_all_topup( cdr_dt, Voucher_Count, bundle_count) 
on tab_user_count.count_date = tab_all_topup.cdr_dt

join (
------------------SMS---COMBINE------OnNet/Int/Nat sms--------------------------------

select 
msg_date_dt
	,sum (CASE WHEN call_type = 1 THEN 1 else 0 end ) as OnNet_sms--count(number_of_sms_charged) as OnNet_SMS
	,sum ( case WHEN call_type = 7 THEN 1 else 0 end ) as Int_sms--count(number_of_sms_charged) as  Int_SMS
	,sum( case when call_type in (1,3,5) THEN 1 else 0 end) as Nat_sms--count(number_of_sms_charged) as nat_sms 	
from uk_rrbs_dm.fact_rrbs_sms frs
where

	  	network_id = 1  
	and 	cdr_types = 50      
	and  	roam_flag in (0,1)   
group by msg_date_dt
order by msg_date_dt
) tab_all_sms (msg_date_dt, OnNet_sms, Int_sms, Nat_sms) 

on tab_user_count.count_date = tab_all_sms.msg_date_dt

JOIN (
------------------------------------------GPRS--------------------------------------------------
select 
	data_termination_dt
	,count (distinct msisdn) as datauser_count
	,sum   (uploaded_bytes +downloaded_bytes)  /1048576 ::decimal(20,4)    as Total_Data_usage

from 	
uk_rrbs_dm.fact_rrbs_gprs frg
where	
Network_ID = 1 
and (uploaded_bytes +downloaded_bytes)>0
and ( RateGroupID <> '1000' or RateGroupId is null)
group by data_termination_dt
--305450
) as tab_data_user(data_termination_dt, datauser_count)

ON 		tab_user_count.count_date	=tab_data_user.data_termination_dt

join uk_rrbs_dm.dim_date dd
on tab_user_count.count_date = dd."date"

join
----------------------------Activation count------------------------------------------------------------------
(
select activationdate, count(1) as activations from uk_rrbs_dm.ref_firstusage rf
group by activationdate ) tab_activation
on tab_activation.activationdate = dd."date" 


;   