---Loading RRBS Data Reference File------
copy uk_rrbs_stg.stg_rrbs_data_cdrtype from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/reference-dimension/RRBS/rrbs_data_cdrType_20200102.csv'
iam_role 'arn:aws:iam::311477489434:role/Redshift-S3-uk-AllSubAccounts'
csv DATEFORMAT 'DD/MM/YYYY' IGNOREHEADER 1; 


copy uk_rrbs_stg.stg_rrbs_data_serviceid from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/reference-dimension/RRBS/rrbs_data_serviceid_20200102.csv'
iam_role 'arn:aws:iam::311477489434:role/Redshift-S3-uk-AllSubAccounts'
csv DATEFORMAT 'DD/MM/YYYY' IGNOREHEADER 1; 

copy uk_rrbs_stg.stg_rrbs_data_subscribertype from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/reference-dimension/RRBS/rrbs_data_subscriberType_20200102.csv'
iam_role 'arn:aws:iam::311477489434:role/Redshift-S3-uk-AllSubAccounts'
csv DATEFORMAT 'DD/MM/YYYY' IGNOREHEADER 1; 

---Loading RRBS SMS Reference File------
copy uk_rrbs_stg.stg_rrbs_sms_calltype from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/reference-dimension/RRBS/rrbs_sms_callType_20200102.csv'
iam_role 'arn:aws:iam::311477489434:role/Redshift-S3-uk-AllSubAccounts'
csv DATEFORMAT 'DD/MM/YYYY' IGNOREHEADER 1; 

copy uk_rrbs_stg.stg_rrbs_sms_cdrtype from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/reference-dimension/RRBS/rrbs_sms_cdrType_20200102.csv'
iam_role 'arn:aws:iam::311477489434:role/Redshift-S3-uk-AllSubAccounts'
csv DATEFORMAT 'DD/MM/YYYY' IGNOREHEADER 1; 

copy uk_rrbs_stg.stg_rrbs_sms_serviceid from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/reference-dimension/RRBS/rrbs_sms_serviceid_20200102.csv'
iam_role 'arn:aws:iam::311477489434:role/Redshift-S3-uk-AllSubAccounts'
csv DATEFORMAT 'DD/MM/YYYY' IGNOREHEADER 1; 


copy uk_rrbs_stg.stg_rrbs_sms_buckettype from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/reference-dimension/RRBS/rrbs_sms_bucketType_20200102.csv'
iam_role 'arn:aws:iam::311477489434:role/Redshift-S3-uk-AllSubAccounts'
csv DATEFORMAT 'DD/MM/YYYY' IGNOREHEADER 1; 

copy uk_rrbs_stg.stg_rrbs_sms_smsfeature from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/reference-dimension/RRBS/rrbs_sms_smsFeature_20200102.csv'
iam_role 'arn:aws:iam::311477489434:role/Redshift-S3-uk-AllSubAccounts'
csv DATEFORMAT 'DD/MM/YYYY' IGNOREHEADER 1; 


copy uk_rrbs_stg.stg_rrbs_sms_subscribertype from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/reference-dimension/RRBS/rrbs_sms_subscriberType_20200102.csv'
iam_role 'arn:aws:iam::311477489434:role/Redshift-S3-uk-AllSubAccounts'
csv DATEFORMAT 'DD/MM/YYYY' IGNOREHEADER 1; 


---Loading RRBS Topup Reference File------
copy uk_rrbs_stg.stg_rrbs_topup_serviceid from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/reference-dimension/RRBS/rrbs_topup_serviceid_20200102.csv'
iam_role 'arn:aws:iam::311477489434:role/Redshift-S3-uk-AllSubAccounts'
csv DATEFORMAT 'DD/MM/YYYY' IGNOREHEADER 1; 

copy uk_rrbs_stg.stg_rrbs_topup_operationcode from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/reference-dimension/RRBS/rrbs_topup_operationcode_20200102.csv'
iam_role 'arn:aws:iam::311477489434:role/Redshift-S3-uk-AllSubAccounts'
csv DATEFORMAT 'DD/MM/YYYY' IGNOREHEADER 1; 

copy uk_rrbs_stg.stg_rrbs_topup_bundlecategory from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/reference-dimension/RRBS/rrbs_topup_bundleCategory_20200102.csv'
iam_role 'arn:aws:iam::311477489434:role/Redshift-S3-uk-AllSubAccounts'
csv DATEFORMAT 'DD/MM/YYYY' IGNOREHEADER 1; 


---Loading RRBS Voice Reference File------
copy uk_rrbs_stg.stg_rrbs_voice_calltype from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/reference-dimension/RRBS/rrbs_voice_callType_20200102.csv'
iam_role 'arn:aws:iam::311477489434:role/Redshift-S3-uk-AllSubAccounts'
csv DATEFORMAT 'DD/MM/YYYY' IGNOREHEADER 1; 

copy uk_rrbs_stg.stg_rrbs_voice_cdrtype from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/reference-dimension/RRBS/rrbs_voice_cdrType_20200102.csv'
iam_role 'arn:aws:iam::311477489434:role/Redshift-S3-uk-AllSubAccounts'
csv DATEFORMAT 'DD/MM/YYYY' IGNOREHEADER 1; 

copy uk_rrbs_stg.stg_rrbs_voice_serviceid from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/reference-dimension/RRBS/rrbs_voice_serviceid_20200102.csv'
iam_role 'arn:aws:iam::311477489434:role/Redshift-S3-uk-AllSubAccounts'
csv DATEFORMAT 'DD/MM/YYYY' IGNOREHEADER 1; 


copy uk_rrbs_stg.stg_rrbs_voice_buckettype from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/reference-dimension/RRBS/rrbs_voice_bucketType_20200102.csv'
iam_role 'arn:aws:iam::311477489434:role/Redshift-S3-uk-AllSubAccounts'
csv DATEFORMAT 'DD/MM/YYYY' IGNOREHEADER 1; 

copy uk_rrbs_stg.stg_rrbs_voice_callfeature from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/reference-dimension/RRBS/rrbs_voice_callfeature_20200102.csv'
iam_role 'arn:aws:iam::311477489434:role/Redshift-S3-uk-AllSubAccounts'
csv DATEFORMAT 'DD/MM/YYYY' IGNOREHEADER 1; 

copy uk_rrbs_stg.stg_rrbs_voice_subscribertype from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/reference-dimension/RRBS/rrbs_voice_subscriberType_20200102.csv'
iam_role 'arn:aws:iam::311477489434:role/Redshift-S3-uk-AllSubAccounts'
csv DATEFORMAT 'DD/MM/YYYY' IGNOREHEADER 1; 

copy uk_rrbs_stg.stg_rrbs_voice_failcause from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/reference-dimension/RRBS/rrbs_voice_failcause_20200102.csv'
iam_role 'arn:aws:iam::311477489434:role/Redshift-S3-uk-AllSubAccounts'
csv DATEFORMAT 'DD/MM/YYYY' IGNOREHEADER 1;

--Loading CodeText Tables------
copy uk_rrbs_stg.stg_rrbs_data_codetext from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/reference-dimension/RRBS/codetext_rrbs_data_20200102.csv'
iam_role 'arn:aws:iam::311477489434:role/Redshift-S3-uk-AllSubAccounts'
csv DATEFORMAT 'DD/MM/YYYY' IGNOREHEADER 1;

copy uk_rrbs_stg.stg_rrbs_voice_codetext from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/reference-dimension/RRBS/codetext_rrbs_voice_20200102.csv'
iam_role 'arn:aws:iam::311477489434:role/Redshift-S3-uk-AllSubAccounts'
csv DATEFORMAT 'DD/MM/YYYY' IGNOREHEADER 1; 

copy uk_rrbs_stg.stg_rrbs_topup_codetext from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/reference-dimension/RRBS/codetext_rrbs_topup_20200102.csv'
iam_role 'arn:aws:iam::311477489434:role/Redshift-S3-uk-AllSubAccounts'
csv DATEFORMAT 'DD/MM/YYYY' IGNOREHEADER 1;

copy uk_rrbs_stg.stg_rrbs_sms_codetext from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/reference-dimension/RRBS/codetext_rrbs_sms_20200102.csv'
iam_role 'arn:aws:iam::311477489434:role/Redshift-S3-uk-AllSubAccounts'
csv DATEFORMAT 'DD/MM/YYYY' IGNOREHEADER 1; 

select * from uk_rrbs_stg.stg_rrbs_data_subscribertype;

select query, filename as filename,line_number as line, 
colname as column, type, position as pos, raw_line as line_text,
raw_field_value as field_text, 
err_reason as reason
from stl_load_errors 
order by query desc
limit 20;





