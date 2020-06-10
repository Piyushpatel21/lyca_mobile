
CREATE TABLE IF NOT EXISTS uk_rrbs_dm.ctrl_rrbs_fact
(
	batch_id INTEGER NOT NULL  ENCODE lzo
	,src_sys VARCHAR(50) NOT NULL  ENCODE lzo
	,filename VARCHAR(100) NOT NULL  ENCODE lzo
	,stgrowcount INTEGER   ENCODE lzo
	,factrowcount INTEGER   ENCODE lzo
	,load_status VARCHAR(100)   ENCODE lzo
	,load_end_time TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::text)::timestamp without time zone ENCODE lzo
)
DISTSTYLE ALL
sortkey(batch_id)
;

