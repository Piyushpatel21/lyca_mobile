/* Work in progress  */

---#sample script to see if there is an error ----------------------------------
select query, filename as filename,line_number as line, 
colname as column, type, position as pos, raw_line as line_text,
raw_field_value as field_text, 
err_reason as reason
from stl_load_errors 
order by query desc
limit 20;


---#Sample copy script to add coloumn with file name----------------------------
create table my_table (
  id integer,
  name varchar(50) NULL
  email varchar(50) NULL,
);

COPY {table_name} FROM 's3://file-key' 
WITH CREDENTIALS 'aws_access_key_id=xxxx;aws_secret_access_key=xxxxx' 
csv
emptyasnull
blanksasnull
DELIMITER ',' ;

ALTER TABLE my_table ADD COLUMN creation_date varchar(256) NOT NULL DEFAULT ;
ALTER TABLE my_table ADD COLUMN processed_file_name varchar(256) NOT NULL DEFAULT '{file-name}';




---## Script to ingest sep month S3 RRBS SMS folder---------------------------------------

copy uk_rrbs_stg.stg_rrbs_sms from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/RRBS/UK/SMS/2019/09'
iam_role 'arn:aws:iam::311477489434:role/Redshift-S3-uk-AllSubAccounts'
csv
emptyasnull
blanksasnull
delimiter ',';

---##Check the total table count and count for each filename-------------------
select count(1) from uk_rrbs_stg.stg_rrbs_sms;
select filename,count(1) from uk_rrbs_stg.stg_rrbs_sms group by filename
order by filename;

---##Sample script to add new coloumns and update filename coloum ---------------
ALTER TABLE uk_rrbs_stg.stg_rrbs_sms ADD COLUMN created_date datetime default sysdate;
ALTER TABLE uk_rrbs_stg.stg_rrbs_sms ADD COLUMN filename varchar(50) NOT NULL DEFAULT 'nofilename';
UPDATE uk_rrbs_stg.stg_rrbs_sms set filename = concat('SMS_',concat(substring(cdr_time_stamp,0,11),'.cdr')) where FileName='nofilename';

---##Sample script to alter table with sortkey ---------------
ALTER TABLE uk_rrbs_stg.stg_rrbs_sms alter SORTKEY (filename);
vacuum;
analyze;


---##Script to ingest sep month RRBS GPRS folder---------------------------------------
copy uk_rrbs_stg.stg_rrbs_gprs from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/RRBS/UK/GPRS/2019/09'
iam_role 'arn:aws:iam::311477489434:role/Redshift-S3-uk-AllSubAccounts'
csv
emptyasnull
blanksasnull
delimiter ',';

---##Check the total table count and count for each filename-------------------
select filename,count(1) from uk_rrbs_stg.stg_rrbs_gprs group by filename
order by filename;

---##Sample script to add new coloumns and update filename coloum ---------------

ALTER TABLE uk_rrbs_stg.stg_rrbs_gprs ADD COLUMN created_date datetime default sysdate;
ALTER TABLE uk_rrbs_stg.stg_rrbs_gprs ADD COLUMN filename varchar(50) NOT NULL DEFAULT 'nofilename';
UPDATE uk_rrbs_stg.stg_rrbs_gprs set filename = concat('GPRS_',concat(substring(cdr_time_stamp,0,11),'.cdr')) where filename = 'nofilename';

---##Sample script to alter table with sortkey ---------------

ALTER TABLE uk_rrbs_stg.stg_rrbs_gprs alter SORTKEY (filename);
vacuum;
analyze; 


---##Script to ingest sep month RRBS Voice folder---------------------------------------
copy uk_rrbs_stg.stg_rrbs_voice from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/RRBS/UK/VOICE/2019/09'
iam_role 'arn:aws:iam::311477489434:role/Redshift-S3-uk-AllSubAccounts'
csv
emptyasnull
blanksasnull
delimiter ',';

---##Check the total table count and count for each filename-------------------
select filename,count(1) from uk_rrbs_stg.stg_rrbs_voice group by filename
order by filename;

---##Sample script to add new coloumns and update filename coloum ---------------

ALTER TABLE uk_rrbs_stg.stg_rrbs_voice ADD COLUMN created_date datetime default sysdate;
ALTER TABLE uk_rrbs_stg.stg_rrbs_voice ADD COLUMN filename varchar(50) NOT NULL DEFAULT 'nofilename';
UPDATE uk_rrbs_stg.stg_rrbs_voice set filename = concat('VOICE_',concat(substring(Call_date,0,11),'.cdr')) where filename = 'nofilename';

---##Sample script to alter table with sortkey ---------------

ALTER TABLE uk_rrbs_stg.stg_rrbs_voice alter SORTKEY (filename);
vacuum;
analyze; 

---##Script to ingest sep month RRBS topup folder---------------------------------------
copy uk_rrbs_stg.stg_rrbs_topup from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/RRBS/UK/TOPUP/2019/09'
iam_role 'arn:aws:iam::311477489434:role/Redshift-S3-uk-AllSubAccounts'
csv
emptyasnull
blanksasnull;

---##Check the total table count and count for each filename-------------------
select filename,count(*) from uk_rrbs_stg.stg_rrbs_topup group by filename
order by filename;

---##Sample script to add new coloumns and update filename coloum ---------------

ALTER TABLE uk_rrbs_stg.stg_rrbs_topup ADD COLUMN created_date datetime default sysdate;
ALTER TABLE uk_rrbs_stg.stg_rrbs_topup ADD COLUMN filename varchar(50) NOT NULL DEFAULT 'nofilename';
UPDATE uk_rrbs_stg.stg_rrbs_topup set filename = concat('TOPUP_',concat(substring(cdr_time_stamp,0,11),'.cdr')) where FileName='nofilename';

---##Sample script to alter table with sortkey ---------------
ALTER TABLE uk_rrbs_stg.stg_rrbs_topup alter SORTKEY (filename);
vacuum;
analyze;


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


