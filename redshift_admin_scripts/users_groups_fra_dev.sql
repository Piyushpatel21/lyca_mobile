/* This file contains the redshift setup including creation of France database, users, groups and their access. */

CREATE DATABASE fradev WITH OWNER ukdevadmin;

--#1 Create Redshift local groups for Cloudwick and Lycamobile teams---------
create group cloudwickgroup;
create group lycagroup;

--#2 Create Redshift local users with passwords------------------------------
--# create user <username> password '<password>' in group <groupname>;
create user despoina_karna password '' in group cloudwickgroup;
create user shubhajit_saha password '' in group cloudwickgroup;
create user sakshi_agarwal password '' in group cloudwickgroup;
create user etl_user password '' in group cloudwickgroup;
create user tejveer_singh password '' in group cloudwickgroup;
create user piyush_patel password '' in group cloudwickgroup;
create user narendra_kumar password '' in group cloudwickgroup;
create user bhavin_tandel password '' in group cloudwickgroup;

create user tharanee_nada password '' in group lycagroup;
create user bala_manik password '' in group lycagroup;
create user thushan_pat password '' in group lycagroup;
create user sendhil_5539 password '' in group lycagroup;
create user srinivasan_5551 password '' in group lycagroup;
create user balaji_5558 password '' in group lycagroup;
create user dhevendran_5579 password '' in group lycagroup;
create user rajesh_5588 password '' in group lycagroup;
create user pavithra_5594 password '' in group lycagroup;
create user suresh_5614 password '' in group lycagroup;
create user karthi_5934 password '' in group lycagroup;
create user saravana_5002 password '' in group lycagroup;
create user manikandan_5435 password '' in group lycagroup;
create user vasanth_5564 password '' in group lycagroup;

--#3 Create redshift schemas ---------------------------------------------------
create schema if not exists fra_rrbs_stg authorization ukdevadmin;
create schema if not exists fra_rrbs_dm authorization ukdevadmin;
create schema if not exists fra_rrbs_rt authorization ukdevadmin;
create schema if not exists fra_mno_dm authorization ukdevadmin;
create schema if not exists fra_accountinfo_dm authorization ukdevadmin;
create schema if not exists fra_logs authorization ukdevadmin;
create schema if not exists fra_test authorization ukdevadmin;

--#4 Grant permission to fradev database to cloudwickgroup--------------------
grant all on DATABASE fradev to group cloudwickgroup;

--#5 Grant permission to the schemas to cloudwickgroup--------------------------
GRANT USAGE ON SCHEMA fra_rrbs_stg TO GROUP cloudwickgroup;
GRANT USAGE ON SCHEMA fra_rrbs_dm TO GROUP cloudwickgroup;
GRANT USAGE ON SCHEMA fra_rrbs_rt TO GROUP cloudwickgroup;
GRANT USAGE ON SCHEMA fra_mno_dm TO GROUP cloudwickgroup;
GRANT USAGE ON SCHEMA fra_accountinfo_dm TO GROUP cloudwickgroup;
GRANT USAGE ON SCHEMA fra_logs TO GROUP cloudwickgroup;
GRANT USAGE ON SCHEMA fra_test TO GROUP cloudwickgroup;

grant all on schema fra_rrbs_stg to group cloudwickgroup;
grant all on schema fra_rrbs_dm to group cloudwickgroup;
grant all on schema fra_rrbs_rt to group cloudwickgroup;
grant all on schema fra_mno_dm TO GROUP cloudwickgroup;
grant all on schema fra_accountinfo_dm TO GROUP cloudwickgroup;
grant all on schema fra_logs TO GROUP cloudwickgroup;
grant all on schema fra_test TO GROUP cloudwickgroup;

grant all on all tables in schema fra_rrbs_stg to group cloudwickgroup;
grant all on all tables in schema fra_rrbs_dm to group cloudwickgroup;
grant all on all tables in schema fra_rrbs_rt to group cloudwickgroup;
grant all on all tables in schema fra_mno_dm to group cloudwickgroup;
grant all on all tables in schema fra_accountinfo_dm to group cloudwickgroup;
grant all on all tables in schema fra_logs to group cloudwickgroup;
grant all on all tables in schema fra_test to group cloudwickgroup;

ALTER DEFAULT PRIVILEGES IN SCHEMA fra_rrbs_stg GRANT ALL ON TABLES TO group cloudwickgroup;
ALTER DEFAULT PRIVILEGES IN SCHEMA fra_rrbs_dm GRANT ALL ON TABLES TO group cloudwickgroup;
ALTER DEFAULT PRIVILEGES IN SCHEMA fra_rrbs_rt GRANT ALL ON TABLES TO group cloudwickgroup;
ALTER DEFAULT PRIVILEGES IN SCHEMA fra_mno_dm GRANT ALL ON TABLES TO group cloudwickgroup;
ALTER DEFAULT PRIVILEGES IN SCHEMA fra_accountinfo_dm GRANT ALL ON TABLES TO group cloudwickgroup;
ALTER DEFAULT PRIVILEGES IN SCHEMA fra_logs GRANT ALL ON TABLES TO group cloudwickgroup;
ALTER DEFAULT PRIVILEGES IN SCHEMA fra_test GRANT ALL ON TABLES TO group cloudwickgroup;

--#10 to Grant super user permission to a user ---------------------------------
alter user <username> createuser;
