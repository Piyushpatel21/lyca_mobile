/* This file contains the redshift setup including creation of databases, users, groups and their access. */
--#1 to create Redshift local groups for Cloudwick and Lycamobile teams---------
create group cloudwickgroup;
create group lycagroup;

--#2 to create Redshift local users with passwords------------------------------
create user <username> password '<password>' in group <groupname>;

--#3 Create redshift schemas ---------------------------------------------------
create schema if not exists uk_rrbs_stg authorization ukdevadmin;
create schema if not exists uk_rrbs_dm authorization ukdevadmin;
create schema if not exists uk_rrbs_rt authorization ukdevadmin;

--#4 Grant permission to ukdev database to cloudwickgroup--------------------
grant all on DATABASE ukdev to group cloudwickgroup;

--#5 Grant permission to the schemas to cloudwickgroup--------------------------
grant all on schema uk_rrbs_stg to group cloudwickgroup;
grant all on schema uk_rrbs_dm to group cloudwickgroup;
grant all on schema uk_rrbs_rt to group cloudwickgroup;

--#6 revoke default create rights on schemas to lycagroup-----------------------
revoke create on schema uk_rrbs_dm from group lycagroup;
revoke create on schema uk_rrbs_stg from group lycagroup;
revoke create on schema uk_rrbs_rt from group lycagroup;

--#7 Grant Usage permission to lycagroup to the schemas-------------------------
GRANT USAGE ON SCHEMA uk_rrbs_dm TO GROUP lycagroup;
GRANT USAGE ON SCHEMA uk_rrbs_stg TO GROUP lycagroup;
GRANT USAGE ON SCHEMA uk_rrbs_rt TO GROUP lycagroup;

--#8 Grant Select permission to lycagroup to the schemas------------------------
GRANT SELECT ON ALL TABLES IN SCHEMA uk_rrbs_stg TO GROUP lycagroup;
GRANT SELECT ON ALL TABLES IN SCHEMA uk_rrbs_dm TO GROUP lycagroup;
GRANT SELECT ON ALL TABLES IN SCHEMA uk_rrbs_rt TO GROUP lycagroup;

--#9 Alter Default Privileges to maintain the permissions on new tables---------
ALTER DEFAULT PRIVILEGES IN SCHEMA "uk_rrbs_stg" GRANT SELECT ON TABLES TO GROUP lycagroup;
ALTER DEFAULT PRIVILEGES IN SCHEMA "uk_rrbs_dm" GRANT SELECT ON TABLES TO GROUP lycagroup;
ALTER DEFAULT PRIVILEGES IN SCHEMA "uk_rrbs_rt" GRANT SELECT ON TABLES TO GROUP lycagroup;

--#10 to Grant super user permission to a user ---------------------------------
alter user <username> createuser;
