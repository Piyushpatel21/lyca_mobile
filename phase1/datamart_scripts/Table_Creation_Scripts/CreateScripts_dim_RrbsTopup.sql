--1

Create table uk_rrbs_dm.dim_rrbs_topup_operationcode
(
sk_operationcode int generated by default as identity (1,1),
operationcode_id smallint,
operationcode_val varchar(100),
param_desc varchar(250),
valid_from timestamp default sysdate not null,
valid_to timestamp DEFAULT '9999-12-31',
is_recent smallint,
batch_id smallint
)
diststyle all
SORTKEY (
	operationcode_id
	)
;


Create table uk_rrbs_dm.dim_rrbs_topup_serviceid
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


Create table uk_rrbs_dm.dim_rrbs_topup_bundleCategory
(
sk_bundleCategory int generated by default as identity (1,1),
bundleCategory_id smallint,
bundleCategory_val varchar(100),
param_desc varchar(250),
valid_from timestamp default sysdate not null,
valid_to timestamp DEFAULT '9999-12-31',
is_recent smallint,
batch_id smallint
)
diststyle all
SORTKEY (
	bundleCategory_id
	)
;

---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS uk_rrbs_dm.dim_rrbs_topup_codetext
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
---------------------------------------------------------------------------