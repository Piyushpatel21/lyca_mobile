---## 1.Script to ingest sep month S3 RRBS SMS folder---------------------------------------

copy uk_rrbs_stg.stg_rrbs_sms from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/RRBS/UK/SMS/2019/09'
iam_role 'arn:aws:iam::311477489434:role/Redshift-S3-uk-AllSubAccounts'
csv
emptyasnull
blanksasnull
delimiter ',';

---##Script to add new coloumns and update filename coloum ---------------
ALTER TABLE uk_rrbs_stg.stg_rrbs_sms ADD COLUMN created_date datetime default sysdate;
ALTER TABLE uk_rrbs_stg.stg_rrbs_sms ADD COLUMN filename varchar(50) NOT NULL DEFAULT 'nofilename';
UPDATE uk_rrbs_stg.stg_rrbs_sms set filename = concat('SMS_',concat(substring(cdr_time_stamp,0,11),'.cdr')) where FileName='nofilename';

---##Script to alter table with sortkey ---------------
ALTER TABLE uk_rrbs_stg.stg_rrbs_sms alter SORTKEY (filename);


---##2. Script to ingest sep month RRBS GPRS folder---------------------------------------
copy uk_rrbs_stg.stg_rrbs_gprs from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/RRBS/UK/GPRS/2019/09'
iam_role 'arn:aws:iam::311477489434:role/Redshift-S3-uk-AllSubAccounts'
csv
emptyasnull
blanksasnull
delimiter ',';


---##Script to add new coloumns and update filename coloum ---------------

ALTER TABLE uk_rrbs_stg.stg_rrbs_gprs ADD COLUMN created_date datetime default sysdate;
ALTER TABLE uk_rrbs_stg.stg_rrbs_gprs ADD COLUMN filename varchar(50) NOT NULL DEFAULT 'nofilename';
UPDATE uk_rrbs_stg.stg_rrbs_gprs set filename = concat('GPRS_',concat(substring(cdr_time_stamp,0,11),'.cdr')) where filename = 'nofilename';

---##Script to alter table with sortkey ---------------

ALTER TABLE uk_rrbs_stg.stg_rrbs_gprs alter SORTKEY (filename);
 


---## 3. Script to ingest sep month RRBS Voice folder---------------------------------------
copy uk_rrbs_stg.stg_rrbs_voice from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/RRBS/UK/VOICE/2019/09'
iam_role 'arn:aws:iam::311477489434:role/Redshift-S3-uk-AllSubAccounts'
csv
emptyasnull
blanksasnull
delimiter ',';


---##Script to add new coloumns and update filename coloum ---------------

ALTER TABLE uk_rrbs_stg.stg_rrbs_voice ADD COLUMN created_date datetime default sysdate;
ALTER TABLE uk_rrbs_stg.stg_rrbs_voice ADD COLUMN filename varchar(50) NOT NULL DEFAULT 'nofilename';
UPDATE uk_rrbs_stg.stg_rrbs_voice set filename = concat('VOICE_',concat(substring(Call_date,0,11),'.cdr')) where filename = 'nofilename';

---##Script to alter table with sortkey ---------------

ALTER TABLE uk_rrbs_stg.stg_rrbs_voice alter SORTKEY (filename);
 

---## 4. Script to ingest sep month RRBS topup folder---------------------------------------
copy uk_rrbs_stg.stg_rrbs_topup from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/RRBS/UK/TOPUP/2019/09'
iam_role 'arn:aws:iam::311477489434:role/Redshift-S3-uk-AllSubAccounts'
csv
emptyasnull
blanksasnull;


---##script to add new coloumns and update filename coloum ---------------

ALTER TABLE uk_rrbs_stg.stg_rrbs_topup ADD COLUMN created_date datetime default sysdate;
ALTER TABLE uk_rrbs_stg.stg_rrbs_topup ADD COLUMN filename varchar(50) NOT NULL DEFAULT 'nofilename';
UPDATE uk_rrbs_stg.stg_rrbs_topup set filename = concat('TOPUP_',concat(substring(cdr_time_stamp,0,11),'.cdr')) where FileName='nofilename';

---##script to alter table with sortkey ---------------
ALTER TABLE uk_rrbs_stg.stg_rrbs_topup alter SORTKEY (filename);


---### Issues found during the ingestion 

--1. Same filename in two different folders only with RRBS-GPRS and this is because of split files recieved from client
/* e.g 
RRBS/UK/GPRS/2019/11/06/GPRS_2019110623.cdr
RRBS/UK/GPRS/2019/11/07/GPRS_2019110623.cdr
*/

/* e.g 
RRBS/UK/GPRS/2019/11/05/GPRS_2019110523.cdr
RRBS/UK/GPRS/2019/11/06/GPRS_2019110523.cdr
*/
-- 2. Extra column found in TOPUP_20192402.cdr file and mannual processing is done for the file

