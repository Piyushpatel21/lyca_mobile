--1

Create table uk_rrbs_dm.dim_rrbs_sms_cdrType
(
sk_cdr_type int generated by default as identity (1,1),
cdrtype_id smallint,
cdrtype_val varchar(100),
param_desc varchar(250),
valid_from timestamp default sysdate not null,
valid_to timestamp DEFAULT '9999-12-31',
is_recent smallint,
batch_id smallint
)
diststyle all
SORTKEY (
	cdrtype_id
	)
;

--2

Create table uk_rrbs_dm.dim_rrbs_sms_callType
(
sk_calltype int generated by default as identity (1,1),
callType_id smallint,
callType_val varchar(100),
param_desc varchar(250),
valid_from timestamp default sysdate not null,
valid_to timestamp DEFAULT '9999-12-31',
is_recent smallint,
batch_id smallint
)
diststyle all
SORTKEY (
	callType_id
	)
;

--3

Create table uk_rrbs_dm.dim_rrbs_sms_serviceid
(
sk_serviceid int generated by default as identity (1,1),
service_id smallint,
serviceid_val varchar(100),
param_desc varchar(250),
valid_from timestamp default sysdate not null,
valid_to timestamp DEFAULT '9999-12-31',
is_recent smallint,
batch_id smallint
)
diststyle all
SORTKEY (
	service_id
	)
;

--4

/*Create table uk_rrbs_dm.dim_rrbs_sms_roamflag
(
sk_roamflag_id int generated by default as identity (1,1),
roamflag_id smallint,
roamflag_val varchar(100),
param_desc varchar(250),
valid_from timestamp default sysdate not null,
valid_to timestamp DEFAULT '9999-12-31',
is_recent smallint,
batch_id smallint
)
diststyle all
SORTKEY (
	roamflag_id
	)
;*/

--5

Create table uk_rrbs_dm.dim_rrbs_sms_smsFeature
(
sk_smsFeature int generated by default as identity (1,1),
smsFeature_id smallint,
smsFeature_val varchar(100),
param_desc varchar(250),
valid_from timestamp default sysdate not null,
valid_to timestamp DEFAULT '9999-12-31',
is_recent smallint,
batch_id smallint
)
diststyle all
SORTKEY (
	smsFeature_id
	)
;

--6

Create table uk_rrbs_dm.dim_rrbs_sms_bucketType
(
sk_bucketType int generated by default as identity (1,1),
bucketType_id smallint,
bucketType_val varchar(100),
param_desc varchar(250),
valid_from timestamp default sysdate not null,
valid_to timestamp DEFAULT '9999-12-31',
is_recent smallint,
batch_id smallint
)
diststyle all
SORTKEY (
	bucketType_id
	)
;

--7

Create table uk_rrbs_dm.dim_rrbs_sms_subscriberType
(
sk_subscriberType int generated by default as identity (1,1),
subscriberType_id smallint,
subscriberType_val varchar(100),
param_desc varchar(250),
valid_from timestamp default sysdate not null,
valid_to timestamp DEFAULT '9999-12-31',
is_recent smallint,
batch_id smallint
)
diststyle all
SORTKEY (
	subscriberType_id
	)
;

-----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS uk_rrbs_dm.dim_rrbs_sms_codetext
(
	sk_codetext INTEGER  DEFAULT default_identity(100947, 0, '1,1'::text) ENCODE lzo
	,code_param VARCHAR(100)   ENCODE RAW
	,code_param_id SMALLINT   ENCODE lzo
	,code_param_val VARCHAR(100)   ENCODE lzo
	,code_param_desc VARCHAR(250)   ENCODE lzo
	,valid_from TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT ('now'::text)::timestamp without time zone ENCODE lzo
	,valid_to TIMESTAMP WITHOUT TIME ZONE  DEFAULT '9999-12-31 00:00:00'::timestamp without time zone ENCODE lzo
	,is_recent SMALLINT   ENCODE lzo
	,batch_id SMALLINT   ENCODE lzo
)
DISTSTYLE ALL
 SORTKEY (
	code_param
	)
;

----------------------------------------------------------------------------
