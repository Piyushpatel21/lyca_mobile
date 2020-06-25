CREATE TABLE uk_rrbs_dm.tgl_mnp_portout_request
(
    id BIGINT encode az64
	,RequestId VARCHAR(30)
	,MSISDN VARCHAR(20)
	,ICCID VARCHAR(30)
	,PACCode VARCHAR(20)
	,PACCreatedDate DATETIME encode az64
	,PACExpiredDate DATETIME encode az64
	,PortOutDate DATETIME encode az64
	,ONO VARCHAR(4)
	,RNO VARCHAR(4)
	,DNO VARCHAR(4)
	,RSP VARCHAR(4)
	,DSP VARCHAR(4)
	,STATUS INT encode az64
	,SMSstate INT encode az64
	,ErrCode VARCHAR(50)
	,RecordLock INT encode az64
	,NumberType VARCHAR(10)
	,Channel VARCHAR(5)
	,Note VARCHAR(100)
	,BlockCode VARCHAR(20)
	,BlockReason VARCHAR(100)
	,PortID VARCHAR(2)
	,CreatedDate DATETIME encode az64
	,CreatedBy VARCHAR(50)
	,CompletedDate DATETIME encode az64
	,PoutFileName VARCHAR(30)
	,RequestFrom VARCHAR(20)
	,PreferredLanguage VARCHAR(20)
	,SmsErrState VARCHAR(10)
	,completeddate_month INT --(yyyymm)
	,batch_id INTEGER
	,created_DATE TIMESTAMP
	,rec_checksum VARCHAR(32)
);

CREATE TABLE uk_rrbs_dm.tgl_mnp_portout_request_duplcdr
(
    id BIGINT encode az64
	,RequestId VARCHAR(30)
	,MSISDN VARCHAR(20)
	,ICCID VARCHAR(30)
	,PACCode VARCHAR(20)
	,PACCreatedDate DATETIME encode az64
	,PACExpiredDate DATETIME encode az64
	,PortOutDate DATETIME encode az64
	,ONO VARCHAR(4)
	,RNO VARCHAR(4)
	,DNO VARCHAR(4)
	,RSP VARCHAR(4)
	,DSP VARCHAR(4)
	,STATUS INT encode az64
	,SMSstate INT encode az64
	,ErrCode VARCHAR(50)
	,RecordLock INT encode az64
	,NumberType VARCHAR(10)
	,Channel VARCHAR(5)
	,Note VARCHAR(100)
	,BlockCode VARCHAR(20)
	,BlockReason VARCHAR(100)
	,PortID VARCHAR(2)
	,CreatedDate DATETIME encode az64
	,CreatedBy VARCHAR(50)
	,CompletedDate DATETIME encode az64
	,PoutFileName VARCHAR(30)
	,RequestFrom VARCHAR(20)
	,PreferredLanguage VARCHAR(20)
	,SmsErrState VARCHAR(10)
	,completeddate_month INT --(yyyymm)
	,batch_id INTEGER
	,created_DATE TIMESTAMP
	,rec_checksum VARCHAR(32)
);