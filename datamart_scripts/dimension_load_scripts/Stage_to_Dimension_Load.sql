call uk_rrbs_dm.sp_rrbs_dimload('rrbs_voice_cdrType_20200102','dim_rrbs_voice_cdrtype','RRBS_Voice_cdrtype');
call uk_rrbs_dm.sp_rrbs_dimload('rrbs_voice_serviceid_20200102','dim_rrbs_voice_serviceid','RRBS_Voice_serviceid');
call uk_rrbs_dm.sp_rrbs_dimload('rrbs_voice_subscriberType_20200102','dim_rrbs_voice_subscribertype','RRBS_Voice_subscribertype');
call uk_rrbs_dm.sp_rrbs_dimload('rrbs_voice_bucketType_20200102','dim_rrbs_voice_buckettype','RRBS_Voice_buckettype');
call uk_rrbs_dm.sp_rrbs_dimload('rrbs_voice_callfeature_20200102','dim_rrbs_voice_callfeature','RRBS_Voice_callfeature');
call uk_rrbs_dm.sp_rrbs_dimload('rrbs_voice_failcause_20200102','dim_rrbs_voice_failcause','RRBS_Voice_failcause');
call uk_rrbs_dm.sp_rrbs_dimload('rrbs_topup_operationcode_20200102','dim_rrbs_topup_operationcode','RRBS_topup_operationcode');
call uk_rrbs_dm.sp_rrbs_dimload('rrbs_topup_serviceid_20200102','dim_rrbs_topup_serviceid','RRBS_topup_serviceid');
call uk_rrbs_dm.sp_rrbs_dimload('rrbs_topup_bundleCategory_20200102','dim_rrbs_topup_bundlecategory','RRBS_topup_bundlecategory');
----Voice CallType------
insert into uk_rrbs_dm.dim_rrbs_voice_calltype
(calltype_id,
cdrtype_id,
calltype_val,
param_desc,
valid_from,
valid_to,
is_recent,
batch_id)
select
param_id,cdrtype_id, param_val,param_desc,sysdate,'9999-12-31',1,(select max(batch_id) +1 from uk_rrbs_dm.ctrl_rrbs_dim) 
from uk_rrbs_stg.stg_rrbs_voice_calltype

insert into uk_rrbs_dm.ctrl_rrbs_dim values((select max(batch_id) +1 from uk_rrbs_dm.ctrl_rrbs_dim) ,'RRBS_voice_calltype','rrbs_voice_calltype_20200102',431,431,'Manual Load',sysdate)

-----------Load into Voice CodeText Files-----------------------
insert into uk_rrbs_dm.dim_rrbs_voice_codetext
(code_param,
code_param_id,
code_param_val,
code_param_desc,
valid_from,
valid_to,
is_recent,
batch_id)
select
code_param,param_id, param_val,param_desc,sysdate,'9999-12-31',1,(select max(batch_id) +1 from uk_rrbs_dm.ctrl_rrbs_dim) 
from uk_rrbs_stg.stg_rrbs_voice_codetext ;

insert into uk_rrbs_dm.ctrl_rrbs_dim values((select max(batch_id) +1 from uk_rrbs_dm.ctrl_rrbs_dim) ,'RRBS_voice_codetext','rrbs_voice_codetext_20200102',40,40,'Manual Load',sysdate);

-----------Load into SMS CodeText Files-----------------------

insert into uk_rrbs_dm.dim_rrbs_sms_codetext
(code_param,
code_param_id,
code_param_val,
code_param_desc,
valid_from,
valid_to,
is_recent,
batch_id)
select
code_param,param_id, param_val,param_desc,sysdate,'9999-12-31',1,(select max(batch_id) +1 from uk_rrbs_dm.ctrl_rrbs_dim) 
from uk_rrbs_stg.stg_rrbs_sms_codetext ;

insert into uk_rrbs_dm.ctrl_rrbs_dim values((select max(batch_id) +1 from uk_rrbs_dm.ctrl_rrbs_dim) ,'RRBS_sms_codetext','rrbs_sms_codetext_20200102',16,16,'Manual Load',sysdate);

-----------Load into Data CodeText Files-----------------------

insert into uk_rrbs_dm.dim_rrbs_data_codetext
(code_param,
code_param_id,
code_param_val,
code_param_desc,
valid_from,
valid_to,
is_recent,
batch_id)
select
code_param,param_id, param_val,param_desc,sysdate,'9999-12-31',1,(select max(batch_id) +1 from uk_rrbs_dm.ctrl_rrbs_dim) 
from uk_rrbs_stg.stg_rrbs_data_codetext ;

insert into uk_rrbs_dm.ctrl_rrbs_dim values((select max(batch_id) +1 from uk_rrbs_dm.ctrl_rrbs_dim) ,'RRBS_data_codetext','rrbs_data_codetext_20200102',11,11,'Manual Load',sysdate)

-----------Load into Topup CodeText Files-----------------------

insert into uk_rrbs_dm.dim_rrbs_topup_codetext
(code_param,
code_param_id,
code_param_val,
code_param_desc,
valid_from,
valid_to,
is_recent,
batch_id)
select
code_param,param_id, param_val,param_desc,sysdate,'9999-12-31',1,(select max(batch_id) +1 from uk_rrbs_dm.ctrl_rrbs_dim) 
from uk_rrbs_stg.stg_rrbs_topup_codetext ;

insert into uk_rrbs_dm.ctrl_rrbs_dim values((select max(batch_id) +1 from uk_rrbs_dm.ctrl_rrbs_dim) ,'RRBS_topup_codetext','rrbs_topup_codetext_20200102',39,39,'Manual Load',sysdate)





