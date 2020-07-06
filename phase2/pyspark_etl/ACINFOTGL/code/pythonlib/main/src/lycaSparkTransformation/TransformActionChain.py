########################################################################
# description     : Data Transformation action chain                   #
# author          : Naren K(narendra.kumar@cloudwick.com),             #
#                   Tejveer Singh(tejveer.singh@cloudwick.com)         #
#                   Shubhajit Saha(shubhajit.saha@cloudwick.com)       #
# contributor     :                                                    #
# version         : 1.0                                                #
# notes           :                                                    #
########################################################################
from typing import Tuple
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from datetime import datetime
from lycaSparkTransformation.DataTransformation import DataTransformation, MnpPortTransformation, MnpSpMasterTransformation,\
    FirstUsageTransformation, DSMRetailerTransformation, MNPONORangeTransformation, MNPLycaspMasterTransformation, \
    MNPONOMasterTransformation, SkipMstCustomerTransformation, SkipMstFreeSimCustomerTransformation, \
    TblShopTransactionStatusTransformation, TrnVoucherActivationTransformation, TrnVouchersTransformation, \
    TrnReCreditTransformation, TrnVouchersUsedTransformation, TrnDebitTransformation, WPTransactionLogTransformation, \
    ReDTransactionLogTransformation, IngenicoTransactionLogTransformation, SRTTrnVouchersLogTransformation, \
    SRTTrnvouchersUsedLogTransformation, RecycledFirstUsageTransformation, MstSwapImsiTransformation, MSTMvnoAccountTransformation, \
    MstFreeSimCustActivationTransformation, MstFreeSimCustomerAddressTransformation, RecycleMasterTransformation, \
    TrnActivationBundleCostTransformation, TrnActivationTransformation

from lycaSparkTransformation.SchemaReader import SchemaReader
from pyspark.sql import DataFrame
from lycaSparkTransformation.JSONBuilder import JSONBuilder
from awsUtils.RedshiftUtils import RedshiftUtils
from awsUtils.AwsReader import AwsReader
from pyspark.sql import functions as py_function


class TransformActionChain:
    def __init__(self, sparkSession: SparkSession, logger, module, subModule, configfile, connfile, run_date, prevDate, code_bucket, encoding):
        self.sparkSession = sparkSession
        self.logger = logger
        self.module = module
        self.subModule = subModule
        self.code_bucket = code_bucket
        self.configfile = AwsReader.s3ReadFile('s3', self.code_bucket, configfile)
        self.connfile = AwsReader.s3ReadFile('s3', self.code_bucket, connfile)
        self.trans = DataTransformation()
        self.run_date = run_date
        self.prevDate = prevDate
        self.jsonParser = JSONBuilder(self.module, self.subModule, self.configfile, self.connfile)
        self.property = self.jsonParser.getAppPrpperty()
        self.connpropery = self.jsonParser.getConnPrpperty()
        self.redshiftprop = RedshiftUtils(self.connpropery.get("host"), self.connpropery.get("port"), self.connpropery.get("domain"),
                                          self.connpropery.get("user"), self.connpropery.get("password"), self.connpropery.get("tmpdir"))
        self.batch_start_dt = datetime.now()
        self.logBatchFileTbl = ".".join([self.property.get("logdb"), self.property.get("batchfiletbl")])
        self.logBatchStatusTbl = ".".join([self.property.get("logdb"), self.property.get("batchstatustbl")])
        self.encoding = encoding

    def getBatchID(self) -> int:
        return self.redshiftprop.getBatchId(self.sparkSession, self.logBatchFileTbl, self.subModule.upper(), self.prevDate)

    def srcSchema(self):
        try:
            self.logger.info("***** generating src, target and checksum schema *****")
            srcSchemaFilePath = AwsReader.s3ReadFile('s3', self.code_bucket, self.property.get("srcSchemaPath"))
            schema = SchemaReader.structTypemapping(srcSchemaFilePath)
            checkSumColumns = self.trans.getCheckSumColumns(srcSchemaFilePath)
            tgtSchemaPath = AwsReader.s3ReadFile('s3', self.code_bucket, self.property.get("tgtSchemaPath"))
            tgtColumns = self.trans.getTgtColumns(tgtSchemaPath)
            self.logger.info("***** return src, target and checksum schema *****")
            return {
                "srcSchema": schema,
                "checkSumColumns": checkSumColumns,
                "tgtSchema": tgtColumns
            }
        except Exception as ex:
            self.logger.error("Failed to create src, tgt, cheksum schema : {error}".format(error=ex))

    def getSourceData(self, batchid, srcSchema, checkSumColumns) -> Tuple[DataFrame, DataFrame, DataFrame]:
        self.logger.info("***** reading source data from s3 *****")
        file_list = self.redshiftprop.getFileList(self.sparkSession, self.logBatchFileTbl, batchid)
        path = self.property.get("sourceFilePath") + "/" + self.module.upper() + "/" + "UK" + "/" +self.subModule.upper() + "/" + self.run_date[:4] + "/" + self.run_date[4:6] + "/" + self.run_date[6:8] + "/"
        fList = []
        s3 = self.property.get("sourceFilePath")
        for file in file_list:
            fList.append(s3 + "/" + file)
        file_list = fList

        try:
            df_source_raw = self.trans.readSourceFile(self.sparkSession, path, srcSchema, batchid, self.encoding, checkSumColumns, file_list)
            df_source_raw.persist()
            if self.property.get("subModule") in ("tgl_mnp_portin_request", "tgl_mnp_portout_request"):
                transformation = MnpPortTransformation()
                df_source_with_datatype = transformation.convertTargetDataType(df_source_raw, srcSchema)
                df_source = transformation.generateDerivedColumnsForMnpPort(df_source_with_datatype)
            elif self.property.get("subModule") == "tgl_mnp_sp_master":
                transformation = MnpSpMasterTransformation()
                df_source_with_datatype = transformation.convertTargetDataType(df_source_raw, srcSchema)
                df_source = transformation.generateDerivedColumnsForMnpSpMaster(df_source_with_datatype)
            elif self.property.get("subModule") == "tgl_first_usage":
                transformation = FirstUsageTransformation()
                df_source_with_datatype = transformation.convertTargetDataType(df_source_raw, srcSchema)
                df_source = transformation.generateDerivedColumnsForFirstUsage(df_source_with_datatype)
            elif self.property.get("subModule") == "tgl_dsm_retailer":
                transformation = DSMRetailerTransformation()
                df_source_with_datatype = transformation.convertTargetDataType(df_source_raw, srcSchema)
                df_source = transformation.generateDerivedColumnsForDSMRetailer(df_source_with_datatype)
            elif self.property.get("subModule") == "tgl_mnp_onorange":
                transformation = MNPONORangeTransformation()
                df_source_with_datatype = transformation.convertTargetDataType(df_source_raw, srcSchema)
                df_source = transformation.generateDerivedColumnsForMNPONORange(df_source_with_datatype)
            elif self.property.get("subModule") == "tgl_mnp_lycasp_master":
                transformation = MNPLycaspMasterTransformation()
                df_source_with_datatype = transformation.convertTargetDataType(df_source_raw, srcSchema)
                df_source = transformation.generateDerivedColumnsForMNPLycaspMaster(df_source_with_datatype)
            elif self.property.get("subModule") == "tgl_mnp_ono_master":
                transformation = MNPONOMasterTransformation()
                df_source_with_datatype = transformation.convertTargetDataType(df_source_raw, srcSchema)
                df_source = transformation.generateDerivedColumnsForMNPONOMaster(df_source_with_datatype)
            elif self.property.get("subModule") == "tgl_mstcustomer":
                transformation = SkipMstCustomerTransformation()
                df_source_with_datatype = transformation.convertTargetDataType(df_source_raw, srcSchema)
                df_source = transformation.generateDerivedColumnsForSkipMstCustomer(df_source_with_datatype)
            elif self.property.get("subModule") == "tgl_mstfreesimcustomer":
                transformation = SkipMstFreeSimCustomerTransformation()
                df_source_with_datatype = transformation.convertTargetDataType(df_source_raw, srcSchema)
                df_source = transformation.generateDerivedColumnsForSkipMstFreeSimCustomer(df_source_with_datatype)
            elif self.property.get("subModule") == "tgl_tble_shop_transaction_status":
                transformation = TblShopTransactionStatusTransformation()
                df_source_with_datatype = transformation.convertTargetDataType(df_source_raw, srcSchema)
                df_source = transformation.generateDerivedColumnsForTblShopTransactionStatus(df_source_with_datatype)
            elif self.property.get("subModule") == "tgl_trn_voucher_activation":
                transformation = TrnVoucherActivationTransformation()
                df_source_with_datatype = transformation.convertTargetDataType(df_source_raw, srcSchema)
                df_source = transformation.generateDerivedColumnsForTrnVoucherActivation(df_source_with_datatype)
            elif self.property.get("subModule") == "tgl_trn_vouchers":
                transformation = TrnVouchersTransformation()
                df_source_with_datatype = transformation.convertTargetDataType(df_source_raw, srcSchema)
                df_source = transformation.generateDerivedColumnsForTrnVouchers(df_source_with_datatype)
            elif self.property.get("subModule") == "tgl_trn_re_credit":
                transformation = TrnReCreditTransformation()
                df_source_with_datatype = transformation.convertTargetDataType(df_source_raw, srcSchema)
                df_source = transformation.generateDerivedColumnsForTrnReCredit(df_source_with_datatype)
            elif self.property.get("subModule") == "tgl_trn_vouchers_used":
                transformation = TrnVouchersUsedTransformation()
                df_source_with_datatype = transformation.convertTargetDataType(df_source_raw, srcSchema)
                df_source = transformation.generateDerivedColumnsForTrnVouchersUsed(df_source_with_datatype)
            elif self.property.get("subModule") == "tgl_trn_debit":
                transformation = TrnDebitTransformation()
                df_source_with_datatype = transformation.convertTargetDataType(df_source_raw, srcSchema)
                df_source = transformation.generateDerivedColumnsForTrnDebit(df_source_with_datatype)
            elif self.property.get("subModule") == "tgl_tbl_wp_transaction_log":
                transformation = WPTransactionLogTransformation()
                df_source_with_datatype = transformation.convertTargetDataType(df_source_raw, srcSchema)
                df_source = transformation.generateDerivedColumnsForWPTransaction(df_source_with_datatype)
            elif self.property.get("subModule") == "tgl_tbl_red_transaction_log":
                transformation = ReDTransactionLogTransformation()
                df_source_with_datatype = transformation.convertTargetDataType(df_source_raw, srcSchema)
                df_source = transformation.generateDerivedColumnsForReDTransactionLog(df_source_with_datatype)
            elif self.property.get("subModule") == "tgl_tbl_ingenico_transaction_log":
                transformation = IngenicoTransactionLogTransformation()
                df_source_with_datatype = transformation.convertTargetDataType(df_source_raw, srcSchema)
                df_source = transformation.generateDerivedColumnsForIngenicoTransactionLog(df_source_with_datatype)
            elif self.property.get("subModule") == "tgl_srt_trn_vouchers_log":
                transformation = SRTTrnVouchersLogTransformation()
                df_source_with_datatype = transformation.convertTargetDataType(df_source_raw, srcSchema)
                df_source = transformation.generateDerivedColumnsForSRTTrnVouchersLog(df_source_with_datatype)
            elif self.property.get("subModule") == "tgl_srt_trnvouchers_used_log":
                transformation = SRTTrnvouchersUsedLogTransformation()
                df_source_with_datatype = transformation.convertTargetDataType(df_source_raw, srcSchema)
                df_source = transformation.generateDerivedColumnsForSRTTrnvouchersUsedLog(df_source_with_datatype)
            elif self.property.get("subModule") == "tgl_recycled_firstusage":
                transformation = RecycledFirstUsageTransformation()
                df_source_with_datatype = transformation.convertTargetDataType(df_source_raw, srcSchema)
                df_source = transformation.generateDerivedColumnsForRecycledFirstUsage(df_source_with_datatype)
            elif self.property.get("subModule") == "tgl_mstswapimsi":
                transformation = MstSwapImsiTransformation()
                df_source_with_datatype = transformation.convertTargetDataType(df_source_raw, srcSchema)
                df_source = transformation.generateDerivedColumnsForMstSwapImsi(df_source_with_datatype)
            elif self.property.get("subModule") == "tgl_mst_mvno_account":
                transformation = MSTMvnoAccountTransformation()
                df_source_with_datatype = transformation.convertTargetDataType(df_source_raw, srcSchema)
                df_source = transformation.generateDerivedColumnsForMSTMvnoAccount(df_source_with_datatype)
            elif self.property.get("subModule") == "tgl_mst_free_sim_customer_activation":
                transformation = MstFreeSimCustActivationTransformation()
                df_source_with_datatype = transformation.convertTargetDataType(df_source_raw, srcSchema)
                df_source = transformation.generateDerivedColumnsForMstFreeSimCustActivation(df_source_with_datatype)
            elif self.property.get("subModule") == "tgl_mst_free_sim_customer_address":
                transformation = MstFreeSimCustomerAddressTransformation()
                df_source_with_datatype = transformation.convertTargetDataType(df_source_raw, srcSchema)
                df_source = transformation.generateDerivedColumnsForMstFreeSimCustomerAddress(df_source_with_datatype)
            elif self.property.get("subModule") == "tgl_recycle_master":
                transformation = RecycleMasterTransformation()
                df_source_with_datatype = transformation.convertTargetDataType(df_source_raw, srcSchema)
                df_source = transformation.generateDerivedColumnsForRecycleMaster(df_source_with_datatype)
            elif self.property.get("subModule") == "tgl_trn_activation":
                transformation = TrnActivationTransformation()
                df_source_with_datatype = transformation.convertTargetDataType(df_source_raw, srcSchema)
                df_source = transformation.generateDerivedColumnsForTrnActivation(df_source_with_datatype)
            elif self.property.get("subModule") == "tgl_trn_activation_bundle_cost":
                transformation = TrnActivationBundleCostTransformation()
                df_source_with_datatype = transformation.convertTargetDataType(df_source_raw, srcSchema)
                df_source = transformation.generateDerivedColumnsForTrnActivationBundleCost(df_source_with_datatype)
            else:
                df_source = df_source_raw
            s3_batchreadcount = df_source.agg(py_function.count('batch_id').cast(IntegerType()).alias('s3_batchreadcount')).rdd.flatMap(lambda row: row).collect()
            s3_filecount = df_source.agg(py_function.countDistinct('filename').cast(IntegerType()).alias('s3_filecount')).rdd.flatMap(lambda row: row).collect()
            batch_status = 'Started'
            metaQuery = ("INSERT INTO {log_batch_status} (BATCH_ID, S3_BATCHREADCOUNT, S3_FILECOUNT, BATCH_STATUS, BATCH_START_DT) values({batch_id},{s3_batchreadcount},{s3_filecount},'{batch_status}','{batch_start_dt}')"
                         .format(log_batch_status=self.logBatchStatusTbl, batch_id=batchid, s3_batchreadcount=''.join(str(e) for e in s3_batchreadcount), s3_filecount=''.join(str(e) for e in s3_filecount), batch_status=batch_status, batch_start_dt=self.batch_start_dt))
            self.redshiftprop.writeBatchStatus(self.sparkSession, self.logBatchStatusTbl, metaQuery)
            df_duplicate = self.trans.getDuplicates(df_source, "rec_checksum")
            batch_status = 'In-Progress'
            intrabatch_dupl_count = df_duplicate.agg(py_function.count('batch_id').cast(IntegerType()).alias('INTRABATCH_DUPL_COUNT')).rdd.flatMap(lambda row: row).collect()
            intrabatch_dist_dupl_count = df_duplicate.select(df_duplicate["rec_checksum"]).distinct().count()
            metaQuery = ("update {log_batch_status} set INTRABATCH_DEDUPL_STATUS='Complete', INTRABATCH_DUPL_COUNT={intrabatch_dupl_count}, BATCH_STATUS='{batch_status}', INTRABATCH_DIST_DUPL_COUNT={intrabatch_dist_dupl_count} where BATCH_ID={batch_id} and BATCH_END_DT is null"
                .format(log_batch_status=self.logBatchStatusTbl, batch_id=batchid, batch_status=batch_status, intrabatch_dupl_count=''.join(str(e) for e in intrabatch_dupl_count), intrabatch_dist_dupl_count=intrabatch_dist_dupl_count))
            self.redshiftprop.writeBatchStatus(self.sparkSession, self.logBatchStatusTbl, metaQuery)
            df_unique = self.trans.getUnique(df_source, "rec_checksum")
            metaQuery = ("update {log_batch_status} set INTRABATCH_NEW_LATE_COUNT={intrabatch_late_count} where batch_id={batch_id} and batch_end_dt is null".format(log_batch_status=self.logBatchStatusTbl, batch_id=batchid, intrabatch_late_count=0))
            self.redshiftprop.writeBatchStatus(self.sparkSession, self.logBatchStatusTbl, metaQuery)
            intrabatch_new_count = df_unique.agg(py_function.count('batch_id').cast(IntegerType()).alias('INTRABATCH_NEW_NORMAL_COUNT')).rdd.flatMap(lambda row: row).collect()
            intrabatch_status = 'Complete'
            metaQuery = ("update {log_batch_status} set INTRABATCH_NEW_NORMAL_COUNT={intrabatch_new_count}, INTRABATCH_DEDUPL_STATUS='{intrabatch_status}' where BATCH_ID={batch_id} and BATCH_END_DT is null".format(log_batch_status=self.logBatchStatusTbl, batch_id=batchid, intrabatch_status=intrabatch_status, intrabatch_new_count=''.join(str(e) for e in intrabatch_new_count)))
            self.redshiftprop.writeBatchStatus(self.sparkSession, self.logBatchStatusTbl, metaQuery)
            self.logger.info("***** source data prepared for transformation *****")
            record_count = df_source.groupBy('filename').agg(py_function.count('batch_id').cast(IntegerType()).alias('RECORD_COUNT'))
            return df_duplicate, df_unique, record_count
        except Exception as ex:
            metaQuery = ("INSERT INTO {log_batch_status} (batch_id,batch_status, batch_start_dt, batch_end_dt) values({batch_id}, '{batch_status}', '{batch_start_dt}', '{batch_end_dt}')".format(log_batch_status=self.logBatchStatusTbl, batch_id=batchid, batch_status='Failed', batch_start_dt=self.batch_start_dt, batch_end_dt=datetime.now()))
            self.redshiftprop.writeBatchStatus(self.sparkSession, self.logBatchStatusTbl, metaQuery)
            self.logger.error("Failed to create source data : {error}".format(error=ex))

    def getDbDuplicate(self) -> DataFrame:
        try:
            self.logger.info("***** reading redshift data to check duplicate *****")
            normalDateRng = int(self.trans.getPrevRangeDate(self.run_date, self.property.get("normalcdrfrq"), self.property.get("numofdayormnthnormal")))
            dfDB = self.redshiftprop.readFromRedshift(self.sparkSession, self.property.get("database"), self.property.get("normalcdrtbl"))
            dfNormalDB = dfDB.filter(py_function.date_format(py_function.col(self.property.get("integerDateColumn")), "yyyyMMdd").cast(IntegerType()) >= normalDateRng)
            self.logger.info("***** mart data is available for duplicate check *****")
            return dfNormalDB
        except Exception as ex:
            self.logger.error("Failed to read redshift for db duplicate : {error}".format(error=ex))

    def getNormalCDR(self, srcDataFrame: DataFrame, normalDBDataFrame: DataFrame, batchid) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
        try:
            self.logger.info("***** generating data for normal cdr *****")
            dfNormalCDRND = self.trans.checkDuplicate(srcDataFrame, normalDBDataFrame)
            dfNormalCDRNewRecord = dfNormalCDRND.filter("newOrDupl == 'New'")
            dfNormalCDRDuplicate = dfNormalCDRND.filter("newOrDupl == 'Duplicate'")
            self.logger.info("***** generating data for normal cdr - completed *****")
            normalcdr_count = dfNormalCDRNewRecord.groupBy('filename').agg(py_function.count('batch_id').cast(IntegerType()).alias('DM_NORMAL_count'))
            normalcdr_dupl_count = dfNormalCDRDuplicate.groupBy('filename').agg(py_function.count('batch_id').cast(IntegerType()).alias('DM_NORMAL_DBDUPL_COUNT'))
            dm_normal_count = dfNormalCDRNewRecord.agg(py_function.count('batch_id').cast(IntegerType()).alias('DM_NORMAL_count')).rdd.flatMap(lambda row: row).collect()
            dm_normal_dupl_count = dfNormalCDRDuplicate.agg(py_function.count('batch_id').cast(IntegerType()).alias('DM_NORMAL_DBDUPL_COUNT')).rdd.flatMap(lambda row: row).collect()
            dm_normal_status = 'Complete'
            metaQuery = ("update {log_batch_status} set DM_NORMAL_STATUS='{dm_normal_status}', DM_NORMAL_COUNT={dm_normal_count}, DM_NORMAL_DBDUPL_COUNT={dm_normal_dupl_count} where BATCH_ID={batch_id} and BATCH_END_DT is null"
                .format(log_batch_status=self.logBatchStatusTbl, batch_id=batchid, dm_normal_status=dm_normal_status, dm_normal_count=''.join(str(e) for e in dm_normal_count),dm_normal_dupl_count=''.join(str(e) for e in dm_normal_dupl_count)))
            self.redshiftprop.writeBatchStatus(self.sparkSession, self.logBatchStatusTbl, metaQuery)
            return dfNormalCDRNewRecord, dfNormalCDRDuplicate, normalcdr_count, normalcdr_dupl_count
        except Exception as ex:
            self.logger.error("Failed to compute Normal CDR data : {error}".format(error=ex))

    def writetoDataMart(self, srcDataframe: DataFrame, tgtColmns=[]):
        try:
            self.logger.info("***** started writing to data mart *****")
            df = srcDataframe.select(*tgtColmns)
            self.redshiftprop.writeToRedshift(df, self.property.get("database"), self.property.get("normalcdrtbl"), tgtColmns)
            self.logger.info("***** started writing to data mart - completed *****")
        except Exception as ex:
            self.logger.error("Failed to write data in data mart : {error}".format(error=ex))

    def writetoDuplicateCDR(self, dataframe: DataFrame, tgtColmns=[]):
        try:
            self.logger.info("***** started writing to duplicate mart *****")
            df = dataframe.select(*tgtColmns)
            self.redshiftprop.writeToRedshift(df, self.property.get("database"), self.property.get("duplicatecdrtbl"), tgtColmns)
            self.logger.info("***** started writing to duplicate mart - completed *****")
        except Exception as ex:
            self.logger.error("Failed to write data in duplicate mart : {error}".format(error=ex))

    def writeBatchFileStatus(self, dataframe : DataFrame, batch_id):
        try:
            self.logger.info("Writing batch status metadata")
            self.redshiftprop.writeBatchFileStatus(self.sparkSession, self.logBatchFileTbl, dataframe, batch_id)
            self.logger.info("Writing batch status metadata - completed")
        except Exception as ex:
            self.logger.error("Failed to write batch status metadata: {error}".format(error=ex))

    def writeBatchStatus(self, batch_id, status):
        batch_end_dt = datetime.now()
        batch_status = status
        metaQuery = ("update {log_batch_status} set BATCH_STATUS='{batch_status}', BATCH_END_DT='{batch_end_dt}' where BATCH_ID = {batch_id} and BATCH_END_DT is null"
            .format(log_batch_status=self.logBatchStatusTbl, batch_status=batch_status, batch_end_dt=batch_end_dt, batch_id=batch_id))
        self.redshiftprop.writeBatchStatus(self.sparkSession, self.logBatchStatusTbl, metaQuery)

