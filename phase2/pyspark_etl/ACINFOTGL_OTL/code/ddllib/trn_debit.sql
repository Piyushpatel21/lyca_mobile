CREATE TABLE uk_rrbs_dm.tgl_trn_debit
(
    Id INT encode az64
    ,Msisdn VARCHAR(20) encode zstd
    ,DebitAmt NUMERIC(38,10) encode az64
    ,TicketId BIGINT encode az64
    ,Reason VARCHAR(20) encode zstd
    ,Comments VARCHAR(200) encode zstd
    ,RequestDate DATETIME encode az64
    ,SubmitedBy VARCHAR(50) encode zstd
    ,STATUS VARCHAR(20) encode zstd
    ,AuthorisedBy VARCHAR(50) encode zstd
    ,AuthorisedDate DATETIME encode az64
    ,OldBal NUMERIC(38,10) encode az64
    ,NewBal NUMERIC(38,10) encode az64
    ,Channel VARCHAR(15) encode zstd
    ,AuthComments VARCHAR(200) encode zstd
    ,batch_id INTEGER
    ,created_DATE TIMESTAMP
    ,rec_checksum VARCHAR(32)
);

CREATE TABLE uk_rrbs_dm.tgl_trn_debit_duplcdr
(
    Id INT encode az64
    ,Msisdn VARCHAR(20) encode zstd
    ,DebitAmt NUMERIC(38,10) encode az64
    ,TicketId BIGINT encode az64
    ,Reason VARCHAR(20) encode zstd
    ,Comments VARCHAR(200) encode zstd
    ,RequestDate DATETIME encode az64
    ,SubmitedBy VARCHAR(50) encode zstd
    ,STATUS VARCHAR(20) encode zstd
    ,AuthorisedBy VARCHAR(50) encode zstd
    ,AuthorisedDate DATETIME encode az64
    ,OldBal NUMERIC(38,10) encode az64
    ,NewBal NUMERIC(38,10) encode az64
    ,Channel VARCHAR(15) encode zstd
    ,AuthComments VARCHAR(200) encode zstd
    ,batch_id INTEGER
    ,created_DATE TIMESTAMP
    ,rec_checksum VARCHAR(32)
);