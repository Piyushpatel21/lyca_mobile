CREATE TABLE uk_rrbs_dm.tgl_mst_free_sim_customer_activation
(
    id int encode az64
    ,transactionid varchar(50) encode zstd
    ,redorderid varchar(50) encode zstd
    ,pinnumber varchar(20) encode zstd
    ,status varchar(3) encode zstd
    ,postcode varchar(100) encode zstd
    ,typeofsim varchar(20) encode zstd
    ,topupamount decimal(38,10) encode az64
    ,cardid varchar(10) encode zstd
    ,nabundleamount decimal(38,10) encode az64
    ,inabundleamount decimal(38,10) encode az64
    ,nabundlecode int encode az64
    ,inabundlecode int encode az64
    ,imsi varchar(20) encode zstd
    ,msisdn_real varchar(20) encode zstd
    ,msisdn_dummy varchar(20) encode zstd
    ,activateddate datetime encode az64
    ,activatedchannel varchar(20) encode zstd
    ,errordesc varchar(200) encode zstd
    ,requestdate datetime encode az64
    ,processeddate datetime encode az64
    ,modeofpayment varchar(20) encode zstd
    ,autorenewal varchar(5) encode zstd
    ,planid int encode az64
    ,promocode varchar(20) encode zstd
    ,promoofferid int encode az64
    ,promotype varchar(2) encode zstd
    ,promodiscounttype varchar(2) encode zstd
    ,promodiscountvalue decimal(38,10) encode az64
    ,shippingamount decimal(38,10) encode az64
    ,shippingpromocode varchar(20) encode zstd
    ,shippingdiscounttype varchar(2) encode zstd
    ,shippingdiscountvalue decimal(38,10) encode az64
    ,shippromodiscountstatus varchar(5) encode zstd
    ,totaltax decimal(38,10) encode az64
    ,tax decimal(38,10) encode az64
    ,obacreditdue decimal(38,10) encode az64
    ,subtransactionid varchar(50) encode zstd
    ,vattransid varchar(16) encode zstd
    ,vatamount decimal(38,10) encode az64
    ,vatperc decimal(5,2) encode az64
    ,bundlemonths int encode az64
    ,appliedmonths int encode az64
    ,firstinstalment decimal(38,10) encode az64
    ,remaininginstalment decimal(38,10) encode az64
    ,amountperterm decimal(38,10) encode az64
    ,issubsidized varchar(10) encode zstd
    ,isobarenewal int encode az64
    ,orderid varchar(50) encode zstd
    ,tax_amt_breakup varchar(200) encode zstd
    ,tax_amt_breakup_desc varchar(500) encode zstd
    ,parentcustomer varchar(20) encode zstd
    ,medium varchar(50) encode zstd
    ,source_name varchar(50) encode zstd
    ,campaign varchar(50) encode zstd
    ,sim_flag smallint encode az64
    ,reason varchar(500) encode zstd
    ,blockedby varchar(50) encode zstd
    ,blockeddate datetime encode az64
    ,fee_breakupdesc varchar(500) encode zstd
    ,fee_breakupamt varchar(500) encode zstd
    ,totalfeeamount decimal(38,2) encode az64
    ,feerefundflag varchar(50) encode zstd
    ,taxrefundflag varchar(50) encode zstd
    ,payment_mode varchar(100) encode zstd
    ,pricesplitup varchar(1000)
    ,calcon varchar(50)
    ,nus_id varchar(15) encode zstd
    ,nus_expiredate datetime encode az64
    ,nus_flag smallint encode az64
    ,nus_applied smallint encode az64
    ,installment_info varchar(500) encode zstd
    ,frequency int encode az64
    ,gateway varchar(150) encode zstd
    ,merchant_id varchar(150) encode zstd
    ,pin_expire_date datetime encode az64
    ,reactivate_count int encode az64
    ,batch_id integer
    ,created_date timestamp
    ,rec_checksum varchar(32)
);

CREATE TABLE uk_rrbs_dm.tgl_mst_free_sim_customer_activation_duplcdr
(
    id int encode az64
    ,transactionid varchar(50) encode zstd
    ,redorderid varchar(50) encode zstd
    ,pinnumber varchar(20) encode zstd
    ,status varchar(3) encode zstd
    ,postcode varchar(100) encode zstd
    ,typeofsim varchar(20) encode zstd
    ,topupamount decimal(38,10) encode az64
    ,cardid varchar(10) encode zstd
    ,nabundleamount decimal(38,10) encode az64
    ,inabundleamount decimal(38,10) encode az64
    ,nabundlecode int encode az64
    ,inabundlecode int encode az64
    ,imsi varchar(20) encode zstd
    ,msisdn_real varchar(20) encode zstd
    ,msisdn_dummy varchar(20) encode zstd
    ,activateddate datetime encode az64
    ,activatedchannel varchar(20) encode zstd
    ,errordesc varchar(200) encode zstd
    ,requestdate datetime encode az64
    ,processeddate datetime encode az64
    ,modeofpayment varchar(20) encode zstd
    ,autorenewal varchar(5) encode zstd
    ,planid int encode az64
    ,promocode varchar(20) encode zstd
    ,promoofferid int encode az64
    ,promotype varchar(2) encode zstd
    ,promodiscounttype varchar(2) encode zstd
    ,promodiscountvalue decimal(38,10) encode az64
    ,shippingamount decimal(38,10) encode az64
    ,shippingpromocode varchar(20) encode zstd
    ,shippingdiscounttype varchar(2) encode zstd
    ,shippingdiscountvalue decimal(38,10) encode az64
    ,shippromodiscountstatus varchar(5) encode zstd
    ,totaltax decimal(38,10) encode az64
    ,tax decimal(38,10) encode az64
    ,obacreditdue decimal(38,10) encode az64
    ,subtransactionid varchar(50) encode zstd
    ,vattransid varchar(16) encode zstd
    ,vatamount decimal(38,10) encode az64
    ,vatperc decimal(5,2) encode az64
    ,bundlemonths int encode az64
    ,appliedmonths int encode az64
    ,firstinstalment decimal(38,10) encode az64
    ,remaininginstalment decimal(38,10) encode az64
    ,amountperterm decimal(38,10) encode az64
    ,issubsidized varchar(10) encode zstd
    ,isobarenewal int encode az64
    ,orderid varchar(50) encode zstd
    ,tax_amt_breakup varchar(200) encode zstd
    ,tax_amt_breakup_desc varchar(500) encode zstd
    ,parentcustomer varchar(20) encode zstd
    ,medium varchar(50) encode zstd
    ,source_name varchar(50) encode zstd
    ,campaign varchar(50) encode zstd
    ,sim_flag smallint encode az64
    ,reason varchar(500) encode zstd
    ,blockedby varchar(50) encode zstd
    ,blockeddate datetime encode az64
    ,fee_breakupdesc varchar(500) encode zstd
    ,fee_breakupamt varchar(500) encode zstd
    ,totalfeeamount decimal(38,2) encode az64
    ,feerefundflag varchar(50) encode zstd
    ,taxrefundflag varchar(50) encode zstd
    ,payment_mode varchar(100) encode zstd
    ,pricesplitup varchar(1000)
    ,calcon varchar(50)
    ,nus_id varchar(15) encode zstd
    ,nus_expiredate datetime encode az64
    ,nus_flag smallint encode az64
    ,nus_applied smallint encode az64
    ,installment_info varchar(500) encode zstd
    ,frequency int encode az64
    ,gateway varchar(150) encode zstd
    ,merchant_id varchar(150) encode zstd
    ,pin_expire_date datetime encode az64
    ,reactivate_count int encode az64
    ,batch_id integer
    ,created_date timestamp
    ,rec_checksum varchar(32)
);