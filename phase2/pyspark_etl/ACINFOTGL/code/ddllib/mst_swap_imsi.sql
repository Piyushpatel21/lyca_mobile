CREATE TABLE uk_rrbs_dm.tgl_mst_swap_imsi
(
    ReqID INT encode az64
    ,OldMSISDN VARCHAR(20) encode zstd
    ,OldICCID VARCHAR(20) encode zstd
    ,OldIMSI VARCHAR(15) encode zstd
    ,NewMSISDN VARCHAR(20) encode zstd
    ,NewICCID VARCHAR(20) encode zstd
    ,NewIMSI VARCHAR(15) encode zstd
    ,TicketID INT encode az64
    ,RequestBy VARCHAR(50) encode zstd
    ,RequestDate DATETIME encode az64
    ,STATUS VARCHAR(4) encode zstd
    ,AuthorisedBy VARCHAR(50) encode zstd
    ,AuthorisedDate DATETIME encode az64
    ,FrequentcalledNumber VARCHAR(15) encode zstd
    ,TopupAmount NUMERIC(38,10) encode az64
    ,TopupDate DATETIME encode az64
    ,CIP VARCHAR(4) encode zstd
    ,PanNumber VARCHAR(20) encode zstd
    ,Admin_Comments VARCHAR(250) encode zstd
    ,FirstName VARCHAR(200) encode zstd
    ,DOB DATETIME encode az64
    ,DocNo VARCHAR(50) encode zstd
    ,DocType VARCHAR(50) encode zstd
    ,DOE DATETIME encode az64
    ,ZIPCODE VARCHAR(40) encode zstd
    ,CHANNEL VARCHAR(40) encode zstd
    ,migration VARCHAR(40) encode zstd
    ,batch_id INTEGER
    ,created_DATE TIMESTAMP
    ,rec_checksum VARCHAR(32)
);

CREATE TABLE uk_rrbs_dm.tgl_mst_swap_imsi_duplcdr
(
    ReqID INT encode az64
    ,OldMSISDN VARCHAR(20) encode zstd
    ,OldICCID VARCHAR(20) encode zstd
    ,OldIMSI VARCHAR(15) encode zstd
    ,NewMSISDN VARCHAR(20) encode zstd
    ,NewICCID VARCHAR(20) encode zstd
    ,NewIMSI VARCHAR(15) encode zstd
    ,TicketID INT encode az64
    ,RequestBy VARCHAR(50) encode zstd
    ,RequestDate DATETIME encode az64
    ,STATUS VARCHAR(4) encode zstd
    ,AuthorisedBy VARCHAR(50) encode zstd
    ,AuthorisedDate DATETIME encode az64
    ,FrequentcalledNumber VARCHAR(15) encode zstd
    ,TopupAmount NUMERIC(38,10) encode az64
    ,TopupDate DATETIME encode az64
    ,CIP VARCHAR(4) encode zstd
    ,PanNumber VARCHAR(20) encode zstd
    ,Admin_Comments VARCHAR(250) encode zstd
    ,FirstName VARCHAR(200) encode zstd
    ,DOB DATETIME encode az64
    ,DocNo VARCHAR(50) encode zstd
    ,DocType VARCHAR(50) encode zstd
    ,DOE DATETIME encode az64
    ,ZIPCODE VARCHAR(40) encode zstd
    ,CHANNEL VARCHAR(40) encode zstd
    ,migration VARCHAR(40) encode zstd
    ,batch_id INTEGER
    ,created_DATE TIMESTAMP
    ,rec_checksum VARCHAR(32)
);