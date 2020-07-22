/* This file contains the redshift setup including creation of UK database, users, groups and their access. */
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
--UK
create schema if not exists uk_rrbs_stg authorization ukdevadmin;
create schema if not exists uk_rrbs_dm authorization ukdevadmin;
create schema if not exists uk_rrbs_rt authorization ukdevadmin;
create schema if not exists uk_mno_dm authorization <username>; 		--change to ukdevadmin
create schema if not exists uk_accountinfo_dm authorization <username>; --change to ukdevadmin
create schema if not exists uk_logs authorization <username>; 			--change to ukdevadmin
create schema if not exists uk_test authorization <username>; 
create schema if not exists uk_log_test authorization <username>; 
create schema if not exists uk_rrbs_agg authorization ukdevadmin;

--#4 Grant permission to ukdev database to cloudwickgroup--------------------
grant all on DATABASE ukdev to group cloudwickgroup;

--#5 Grant permission to the schemas to cloudwickgroup--------------------------

--UK
GRANT USAGE ON SCHEMA uk_rrbs_stg TO GROUP cloudwickgroup;
GRANT USAGE ON SCHEMA uk_rrbs_dm TO GROUP cloudwickgroup;
GRANT USAGE ON SCHEMA uk_rrbs_rt TO GROUP cloudwickgroup;
GRANT USAGE ON SCHEMA uk_mno_dm TO GROUP cloudwickgroup;
GRANT USAGE ON SCHEMA uk_accountinfo_dm TO GROUP cloudwickgroup;
GRANT USAGE ON SCHEMA uk_logs TO GROUP cloudwickgroup;
GRANT USAGE ON SCHEMA uk_test TO GROUP cloudwickgroup;
GRANT USAGE ON SCHEMA uk_log_test TO GROUP cloudwickgroup;
GRANT USAGE ON SCHEMA uk_rrbs_agg TO GROUP CLOUDWICKGROUP;


GRANT ALL ON SCHEMA uk_rrbs_stg to GROUP cloudwickgroup;
GRANT ALL ON SCHEMA uk_rrbs_dm to GROUP cloudwickgroup;
GRANT ALL ON SCHEMA uk_rrbs_rt to GROUP cloudwickgroup;
GRANT ALL ON SCHEMA uk_mno_dm TO GROUP cloudwickgroup;
GRANT ALL ON SCHEMA uk_accountinfo_dm TO GROUP cloudwickgroup;
GRANT ALL ON SCHEMA uk_logs TO GROUP cloudwickgroup;
GRANT ALL ON SCHEMA uk_test TO GROUP cloudwickgroup;
GRANT ALL ON SCHEMA uk_log_test TO GROUP cloudwickgroup;
GRANT ALL ON SCHEMA uk_rrbs_agg to GROUP cloudwickgroup;


GRANT ALL ON ALL tables in schema uk_rrbs_stg to group cloudwickgroup;
GRANT ALL ON ALL tables in schema uk_rrbs_dm to group cloudwickgroup;
GRANT ALL ON ALL tables in schema uk_rrbs_rt to group cloudwickgroup;
GRANT ALL ON ALL tables in schema uk_mno_dm to group cloudwickgroup;
GRANT ALL ON ALL tables in schema uk_accountinfo_dm to group cloudwickgroup;
GRANT ALL ON ALL tables in schema uk_logs to group cloudwickgroup;
GRANT ALL ON ALL tables in schema uk_test to group cloudwickgroup;
GRANT ALL ON ALL tables in schema uk_log_test to group cloudwickgroup;
GRANT ALL ON all tables in schema uk_rrbs_agg to GROUP cloudwickgroup;


ALTER DEFAULT PRIVILEGES IN SCHEMA uk_rrbs_stg GRANT ALL ON TABLES TO group cloudwickgroup;
ALTER DEFAULT PRIVILEGES IN SCHEMA uk_rrbs_dm GRANT ALL ON TABLES TO group cloudwickgroup;
ALTER DEFAULT PRIVILEGES IN SCHEMA uk_rrbs_rt GRANT ALL ON TABLES TO group cloudwickgroup;
ALTER DEFAULT PRIVILEGES IN SCHEMA uk_mno_dm GRANT ALL ON TABLES TO group cloudwickgroup;
ALTER DEFAULT PRIVILEGES IN SCHEMA uk_accountinfo_dm GRANT ALL ON TABLES TO group cloudwickgroup;
ALTER DEFAULT PRIVILEGES IN SCHEMA uk_logs GRANT ALL ON TABLES TO group cloudwickgroup;
ALTER DEFAULT PRIVILEGES IN SCHEMA uk_test GRANT ALL ON TABLES TO group cloudwickgroup;
ALTER DEFAULT PRIVILEGES IN SCHEMA uk_log_test GRANT ALL ON TABLES TO group cloudwickgroup;

--#6 Revoke default rights on schemas and tables to lycagroup-----------------------
revoke all on all tables in schema uk_rrbs_stg from group lycagroup;
revoke all on all tables in schema uk_rrbs_dm from group lycagroup;
revoke all on all tables in schema uk_rrbs_rt from group lycagroup;
revoke all on all tables in schema uk_rrbs_stg from group lycagroup;
revoke all on all tables in schema uk_rrbs_dm from group lycagroup;
revoke all on all tables in schema uk_rrbs_rt from group lycagroup;
revoke all on all tables in schema uk_mno_dm from group lycagroup;
revoke all on all tables in schema uk_accountinfo_dm from group lycagroup;
revoke all on all tables in schema uk_logs from group lycagroup;
revoke all on all tables in schema uk_test from group lycagroup;
revoke all on all tables in schema uk_log_test from group lycagroup;
revoke create on schema uk_rrbs_dm from group lycagroup;
revoke create on schema uk_rrbs_stg from group lycagroup;
revoke create on schema uk_rrbs_rt from group lycagroup;
revoke create on schema uk_mno_dm from group lycagroup;
revoke create on schema uk_accountinfo_dm from group lycagroup;
revoke create on schema uk_logs from group lycagroup;
revoke create on schema uk_test from group lycagroup;
revoke create on schema uk_log_test from group lycagroup;

--#7 Grant Usage permission to lycagroup to the schemas-------------------------
GRANT USAGE ON SCHEMA uk_rrbs_dm TO GROUP lycagroup;
GRANT USAGE ON SCHEMA uk_rrbs_stg TO GROUP lycagroup;
GRANT USAGE ON SCHEMA uk_rrbs_rt TO GROUP lycagroup;
GRANT USAGE ON SCHEMA uk_mno_dm TO GROUP lycagroup;
GRANT USAGE ON SCHEMA uk_accountinfo_dm TO GROUP lycagroup;

--#8 Grant Select permission to lycagroup to the schemas------------------------
GRANT SELECT ON ALL TABLES IN SCHEMA uk_rrbs_stg TO GROUP lycagroup;
GRANT SELECT ON ALL TABLES IN SCHEMA uk_rrbs_dm TO GROUP lycagroup;
GRANT SELECT ON ALL TABLES IN SCHEMA uk_rrbs_rt TO GROUP lycagroup;
GRANT SELECT ON ALL TABLES IN SCHEMA uk_mno_dm TO GROUP lycagroup;
GRANT SELECT ON ALL TABLES IN SCHEMA uk_accountinfo_dm TO GROUP lycagroup;

--#9 Alter Default Privileges to maintain the permissions on new tables---------
ALTER DEFAULT PRIVILEGES IN SCHEMA "uk_rrbs_stg" GRANT SELECT ON TABLES TO GROUP lycagroup;
ALTER DEFAULT PRIVILEGES IN SCHEMA "uk_rrbs_dm" GRANT SELECT ON TABLES TO GROUP lycagroup;
ALTER DEFAULT PRIVILEGES IN SCHEMA "uk_rrbs_rt" GRANT SELECT ON TABLES TO GROUP lycagroup;
ALTER DEFAULT PRIVILEGES IN SCHEMA "uk_mno_dm" GRANT SELECT ON TABLES TO GROUP lycagroup;
ALTER DEFAULT PRIVILEGES IN SCHEMA "uk_accountinfo_dm" GRANT SELECT ON TABLES TO GROUP lycagroup;

--#10 to Grant super user permission to a user ---------------------------------
alter user <username> createuser;
