CREATE TABLE uk_rrbs_dm.tgl_mst_mvno_account
(
    id BIGINT encode az64
    ,MSISDN VARCHAR(20) encode zstd
    ,MSISDN_Swap VARCHAR(20) encode zstd
    ,custcode VARCHAR(8) encode zstd
    ,batchcode INT encode az64
    ,serialcode INT encode az64
    ,iccidprefix VARCHAR(8) encode zstd
    ,iccid VARCHAR(20) encode zstd
    ,IMSI VARCHAR(20) encode zstd
    ,SwapIMSI VARCHAR(20) encode zstd
    ,IMSI2 VARCHAR(20) encode zstd
    ,pin1 VARCHAR(10) encode zstd
    ,pin2 VARCHAR(10) encode zstd
    ,puk1 VARCHAR(8) encode zstd
    ,puk2 VARCHAR(8) encode zstd
    ,SCPSubmitState VARCHAR(1) encode zstd
    ,SCPErrCode VARCHAR(4) encode zstd
    ,RecordLock VARCHAR(1) encode zstd
    ,Processid INT encode az64
    ,STATUS INT encode az64
    ,PIM INT encode az64
    ,MSISDN_Flag SMALLINT encode az64
    ,Tariffclassname VARCHAR(20) encode zstd
    ,Initialbal VARCHAR(50) encode zstd
    ,Initiallang VARCHAR(50) encode zstd
    ,MO_Calls INT encode az64
    ,MT_Calls INT encode az64
    ,MO_ROAM_Calls INT encode az64
    ,MT_ROAM_Calls INT encode az64
    ,MO_SMS INT encode az64
    ,MT_SMS INT encode az64
    ,MO_ROAM_SMS INT encode az64
    ,MT_ROAM_SMS INT encode az64
    ,IVR INT encode az64
    ,USSD INT encode az64
    ,VMS INT encode az64
    ,SMS_TOPUP INT encode az64
    ,MHA INT encode az64
    ,MCA INT encode az64
    ,GPRS INT encode az64
    ,CRBT INT encode az64
    ,HLRID VARCHAR(10) encode zstd
    ,MHAAccid VARCHAR(20) encode zstd
    ,MHApin VARCHAR(20) encode zstd
    ,SMSMarketing INT encode az64
    ,ActDate DATETIME encode az64
    ,iccidcomputed VARCHAR(15) encode zstd
    ,CIP VARCHAR(4) encode zstd
    ,Mask_Mode SMALLINT encode az64
    ,EATariffChangeStatus INT encode az64
    ,EATariffclassname VARCHAR(15) encode zstd
    ,SuspendSIMSwapRequest SMALLINT encode az64
    ,SuspendMSISDNSwapRequest SMALLINT encode az64
    ,SuspendPortOutRequest SMALLINT encode az64
    ,Resellerid VARCHAR(25) encode zstd
    ,batch_id INTEGER
    ,created_DATE TIMESTAMP
    ,rec_checksum VARCHAR(32)
);

CREATE TABLE uk_rrbs_dm.tgl_mst_mvno_account_duplcdr
(
    id BIGINT encode az64
    ,MSISDN VARCHAR(20) encode zstd
    ,MSISDN_Swap VARCHAR(20) encode zstd
    ,custcode VARCHAR(8) encode zstd
    ,batchcode INT encode az64
    ,serialcode INT encode az64
    ,iccidprefix VARCHAR(8) encode zstd
    ,iccid VARCHAR(20) encode zstd
    ,IMSI VARCHAR(20) encode zstd
    ,SwapIMSI VARCHAR(20) encode zstd
    ,IMSI2 VARCHAR(20) encode zstd
    ,pin1 VARCHAR(10) encode zstd
    ,pin2 VARCHAR(10) encode zstd
    ,puk1 VARCHAR(8) encode zstd
    ,puk2 VARCHAR(8) encode zstd
    ,SCPSubmitState VARCHAR(1) encode zstd
    ,SCPErrCode VARCHAR(4) encode zstd
    ,RecordLock VARCHAR(1) encode zstd
    ,Processid INT encode az64
    ,STATUS INT encode az64
    ,PIM INT encode az64
    ,MSISDN_Flag SMALLINT encode az64
    ,Tariffclassname VARCHAR(20) encode zstd
    ,Initialbal VARCHAR(50) encode zstd
    ,Initiallang VARCHAR(50) encode zstd
    ,MO_Calls INT encode az64
    ,MT_Calls INT encode az64
    ,MO_ROAM_Calls INT encode az64
    ,MT_ROAM_Calls INT encode az64
    ,MO_SMS INT encode az64
    ,MT_SMS INT encode az64
    ,MO_ROAM_SMS INT encode az64
    ,MT_ROAM_SMS INT encode az64
    ,IVR INT encode az64
    ,USSD INT encode az64
    ,VMS INT encode az64
    ,SMS_TOPUP INT encode az64
    ,MHA INT encode az64
    ,MCA INT encode az64
    ,GPRS INT encode az64
    ,CRBT INT encode az64
    ,HLRID VARCHAR(10) encode zstd
    ,MHAAccid VARCHAR(20) encode zstd
    ,MHApin VARCHAR(20) encode zstd
    ,SMSMarketing INT encode az64
    ,ActDate DATETIME encode az64
    ,iccidcomputed VARCHAR(15) encode zstd
    ,CIP VARCHAR(4) encode zstd
    ,Mask_Mode SMALLINT encode az64
    ,EATariffChangeStatus INT encode az64
    ,EATariffclassname VARCHAR(15) encode zstd
    ,SuspendSIMSwapRequest SMALLINT encode az64
    ,SuspendMSISDNSwapRequest SMALLINT encode az64
    ,SuspendPortOutRequest SMALLINT encode az64
    ,Resellerid VARCHAR(25) encode zstd
    ,batch_id INTEGER
    ,created_DATE TIMESTAMP
    ,rec_checksum VARCHAR(32)
);