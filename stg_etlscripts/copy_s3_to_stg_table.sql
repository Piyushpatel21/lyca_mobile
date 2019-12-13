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



