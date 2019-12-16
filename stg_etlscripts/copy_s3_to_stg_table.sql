/* Work in progress  */
--#Sample scripts---------------------------------------------------------------
copy ukdev.uk_dev_stg.stg_rrbs_uk_voice from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/RRBS/UK/VOICE/2019/11/07/VOICE_2019110722.cdr'
iam_role 'arn:aws:iam::311477489434:role/Redshift-S3-uk-AllSubAccounts'
csv
delimiter ','; 


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
DATEFORMAT 'auto' TIMEFORMAT 'auto' MAXERROR 0 ACCEPTINVCHARS '*' DELIMITER '\t' GZIP;

ALTER TABLE my_table ADD COLUMN processed_file_name varchar(256) NOT NULL DEFAULT '{file-name}';




---##Sample script to fix the extra coloumn issue ------------------------------
copy ukdev.stg.temp_stg_VOICE_2019110723 from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/RRBS/UK/VOICE/2019/11/07/VOICE_2019110723.cdr'
iam_role 'arn:aws:iam::311477489434:role/Redshift-S3-uk-AllSubAccounts'
delimiter ','
removequotes
emptyasnull
blanksasnull
maxerror 5;

---## Script to ingest complete S3 folder---------------------------------------

copy ukdev.stg.stg_rrbs_uk_voice from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/RRBS/UK/VOICE/'
iam_role 'arn:aws:iam::311477489434:role/Redshift-S3-uk-AllSubAccounts'
csv
delimiter ',';

---## cdr_time_stamp validation which is used to create filename=---------------
select cdr_time_stamp from ukdev.stg.stg_rrbs_uk_voice limit 50;


---##Sample script to add new coloumns and update filename coloum ---------------
ALTER TABLE ukdev.stg.stg_rrbs_uk_voice ADD COLUMN FileName varchar(50) NOT NULL DEFAULT 'nofilename';
UPDATE ukdev.stg.stg_rrbs_uk_voice set FileName = concat('VOICE_',concat(substring(cdr_time_stamp,0,11),'.cdr')) where FileName='nofilename';
ALTER TABLE ukdev.stg.stg_rrbs_uk_voice ADD COLUMN Created_Date datetime default sysdate;

---##Sample script to alter table with sortkey ---------------
ALTER TABLE ukdev.stg.stg_rrbs_uk_voice alter SORTKEY (FileName);

---### Issues found during the ingestion 

---1.-###S3 data from 's3://mis-dl-uk-eu-west-2-311477489434-dev-raw/RRBS/UK/VOICE/2019/11/07' has data from future dates
cdr_time_stamp= 2019110800* = 2,549 records
cdr_time_stamp= 2019110801* = 216 records
cdr_time_stamp= 2019111302* = 24 records
cdr_time_stamp= 2019111303* = 7 records

---### Removed those rows using the following commands 
delete from ukdev.stg.stg_rrbs_uk_voice where FileName='VOICE_2019110800.cdr';
delete from ukdev.stg.stg_rrbs_uk_voice where FileName='VOICE_2019110801.cdr';
delete from ukdev.stg.stg_rrbs_uk_voice where FileName='VOICE_2019111302.cdr';
delete from ukdev.stg.stg_rrbs_uk_voice where FileName='VOICE_2019111303.cdr';


---2.-###S3 data has same file names in two different folders only in RRBS-GPRS--
/* e.g 
RRBS/UK/GPRS/2019/11/06/GPRS_2019110623.cdr
RRBS/UK/GPRS/2019/11/07/GPRS_2019110623.cdr
*/

/* e.g 
RRBS/UK/GPRS/2019/11/05/GPRS_2019110523.cdr
RRBS/UK/GPRS/2019/11/06/GPRS_2019110523.cdr
*/

/*
RRBS/UK/GPRS/2019/11/03/GPRS_2019110323.cdr
RRBS/UK/GPRS/2019/11/04/GPRS_2019110323.cdr
*/
/*
RRBS/UK/GPRS/2019/11/02/GPRS_2019110223.cdr
RRBS/UK/GPRS/2019/11/03/GPRS_2019110223.cdr
*/
/*
RRBS/UK/GPRS/2019/11/01/GPRS_2019110123.cdr
RRBS/UK/GPRS/2019/11/02/GPRS_2019110123.cdr
*/


