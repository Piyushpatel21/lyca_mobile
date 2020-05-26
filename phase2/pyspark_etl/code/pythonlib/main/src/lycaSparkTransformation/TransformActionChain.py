########################################################################
# description     : processing JSON config files.                      #
# author          : Naren K(narendra.kumar@cloudwick.com),             #
#                   Tejveer Singh(tejveer.singh@cloudwick.com)         #
#                   Shubhajit Saha(shubhajit.saha@cloudwick.com)       #
# contributor     :                                                    #
# version         : 1.0                                                #
# notes           :                                                    #
########################################################################
from typing import Tuple
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from datetime import datetime
from lycaSparkTransformation.DataTransformation import DataTransformation
from lycaSparkTransformation.SchemaReader import SchemaReader
from pyspark.sql import DataFrame
from lycaSparkTransformation.JSONBuilder import JSONBuilder
from awsUtils.RedshiftUtils import RedshiftUtils
from awsUtils.AwsReader import AwsReader
from pyspark.sql import functions as py_function


class TransformActionChain:
    def __init__(self, logger, module, subModule, configfile, connfile, run_date, batch_from=None, batch_to=None):
        self.logger = logger
        self.module = module
        self.subModule = subModule
        self.batch_from = batch_from
        self.batch_to = batch_to
        self.configfile = AwsReader.s3ReadFile('s3', 'aws-glue-temporary-484320814466-eu-west-2', configfile)
        self.connfile = AwsReader.s3ReadFile('s3', 'aws-glue-temporary-484320814466-eu-west-2', connfile)
        self.trans = DataTransformation()
        self.run_date = run_date
        self.jsonParser = JSONBuilder(self.module, self.subModule, self.configfile, self.connfile)
        self.property = self.jsonParser.getAppPrpperty()
        self.connpropery = self.jsonParser.getConnPrpperty()
        self.redshiftprop = RedshiftUtils(self.connpropery.get("host"), self.connpropery.get("port"), self.connpropery.get("domain"),
                                          self.connpropery.get("user"), self.connpropery.get("password"), self.connpropery.get("tmpdir"))

    def getstatus(self, sparkSession: SparkSession, batch_ID: int, column, status) -> DataFrame:
        schema = StructType([StructField('batch_ID', IntegerType(), True), StructField(column, StringType(), True)])
        data = [(batch_ID, status)]
        rdd = sparkSession.sparkContext.parallelize(data)
        return sparkSession.createDataFrame(rdd, schema)

    def getdmCNT(self, sparkSession: SparkSession, batch_ID: int, column, cnt: int) -> DataFrame:
        schema = StructType([StructField('batch_ID', IntegerType(), True), StructField(column, IntegerType(), True)])
        data = [(batch_ID, cnt)]
        rdd = sparkSession.sparkContext.parallelize(data)
        return sparkSession.createDataFrame(rdd, schema)

    def getBatchID(self, sparkSession: SparkSession) -> int:
        return self.redshiftprop.getBatchId(sparkSession, self.subModule.upper(), self.batch_from, self.batch_to)

    def srcSchema(self):
        try:
            self.logger.info("***** generating src, target and checksum schema *****")
            srcSchemaFilePath = AwsReader.s3ReadFile('s3', 'aws-glue-temporary-484320814466-eu-west-2', self.property.get("srcSchemaPath"))
            schema = SchemaReader.structTypemapping(srcSchemaFilePath)
            checkSumColumns = self.trans.getCheckSumColumns(srcSchemaFilePath)
            tgtSchemaPath = AwsReader.s3ReadFile('s3', 'aws-glue-temporary-484320814466-eu-west-2', self.property.get("tgtSchemaPath"))
            tgtColumns = self.trans.getTgtColumns(tgtSchemaPath)
            self.logger.info("***** return src, target and checksum schema *****")
            return {
                "srcSchema": schema,
                "checkSumColumns": checkSumColumns,
                "tgtSchema": tgtColumns
            }
        except Exception as ex:
            self.logger.error("Failed to create src, tgt, cheksum schema : {error}".format(error=ex))

    def getSourceData(self, sparkSession: SparkSession, batchid, srcSchema, checkSumColumns) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
        try:
            BATCH_START_DT = datetime.now()
            self.logger.info("***** reading source data from s3 *****")
            # file_list = self.redshiftprop.getFileList(sparkSession, batchid)
            file_list = ['new_sample_file.cdr']
            prmryKey = "sk_rrbs_" + self.subModule
            # To run on Glue
            #path = self.property.get("sourceFilePath") + "/" + self.module.upper() + "/" + "UK" + "/" +self.subModule.upper() + "/" + self.run_date[:4] + "/" + self.run_date[4:6] + "/" + self.run_date[6:8] + "/"
            # To run Locally
            path = '/Users/bhavintandel/Documents/Documents/Cloudwick/Projects/lyca/lycamobile-etl-movements/phase2/pyspark_etl/code/pythonlib/test/resources/'
            df_source = self.trans.readSourceFile(sparkSession, path, srcSchema, batchid, prmryKey, checkSumColumns, file_list)
            date_range = int(self.trans.getPrevRangeDate(self.run_date, self.property.get("normalcdrfrq"), self.property.get("numofdayormnthnormal")))
            lateOrNormalCdr = self.trans.getLateOrNormalCdr(df_source, self.property.get("dateColumn"), self.property.get("formattedDateColumn"), self.property.get("integerDateColumn"), date_range)
            df_duplicate = self.trans.getDuplicates(lateOrNormalCdr, "rec_checksum")
            df_unique_late = self.trans.getUnique(lateOrNormalCdr, "rec_checksum").filter("normalOrlate == 'Late'")
            df_unique_normal = self.trans.getUnique(lateOrNormalCdr, "rec_checksum").filter("normalOrlate == 'Normal'")
            self.logger.info("***** source data prepared for transformation *****")
            record_count = df_source.groupBy('filename').agg(py_function.count('batch_id').alias('record_count').cast(IntegerType()))
            LOG_BATCH_STATUS = df_source.groupBy('batch_id').agg(py_function.count('batch_id').cast(IntegerType()).alias('s3_batchreadcount')
                                               ,py_function.countDistinct('filename').cast(IntegerType()).alias('s3_filecount')) \
                                               .withColumn('batch_start_dt', py_function.lit(BATCH_START_DT)) \
                                               .withColumn('metadata_status', py_function.lit('P')) \
                                               .withColumn('intrabatch_dedup_status', py_function.lit('C')) \
                                               .withColumn('intrabatch_duplicate_count', py_function.lit(df_duplicate.count()))
            return df_duplicate, df_unique_late, df_unique_normal, record_count
        except Exception as ex:
            self.logger.error("Failed to create source data : {error}".format(error=ex))

    def getDbDuplicate(self, sparkSession: SparkSession) -> Tuple[DataFrame, DataFrame]:
        try:
            self.logger.info("***** reading redshift data to check duplicate *****")
            normalDateRng = int(self.trans.getPrevRangeDate(self.run_date, self.property.get("normalcdrfrq"), self.property.get("numofdayormnthnormal")))
            lateDateRng = int(self.trans.getPrevRangeDate(self.run_date, self.property.get("latecdrfrq"), self.property.get("numofdayormnthlate")))
            dfDB = self.redshiftprop.readFromRedshift(sparkSession, self.property.get("database"), self.property.get("normalcdrtbl"))
            dfNormalDB = dfDB.filter(dfDB[self.property.get("integerDateColumn")] >= normalDateRng)
            dfLateDB = dfDB.filter(dfDB[self.property.get("integerDateColumn")] >= lateDateRng)
            self.logger.info("***** mart data is available for duplicate check *****")
            return dfNormalDB, dfLateDB
        except Exception as ex:
            self.logger.error("Failed to read redshift for db duplicate : {error}".format(error=ex))

    def getLateCDR(self, srcDataFrame: DataFrame, lateDBDataFrame: DataFrame) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
        try:
            self.logger.info("***** generating data for late cdr *****")
            dfLateCDRND = self.trans.checkDuplicate(srcDataFrame, lateDBDataFrame)
            dfLateCDRNewRecord = dfLateCDRND.filter("newOrDupl == 'New'")
            dfLateCDRDuplicate = dfLateCDRND.filter("newOrDupl == 'Duplicate'")
            self.logger.info("***** generating data for late cdr - completed *****")
            latecdr_count = dfLateCDRNewRecord.groupBy('filename').agg(py_function.count('batch_id').alias('latecdr_dm_count').cast(IntegerType()),
                                                                       py_function.count('batch_id').alias('latecdr_lm_count').cast(IntegerType()))
            latecdr_dupl_count = dfLateCDRDuplicate.groupBy('filename').agg(py_function.count('batch_id').alias('latecdr_duplicate_count').cast(IntegerType()))
            return dfLateCDRNewRecord, dfLateCDRDuplicate, latecdr_count, latecdr_dupl_count
        except Exception as ex:
            self.logger.error("Failed to compute Late CDR data : {error}".format(error=ex))

    def getNormalCDR(self, srcDataFrame: DataFrame, normalDBDataFrame: DataFrame) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
        try:
            self.logger.info("***** generating data for normal cdr *****")
            dfNormalCDRND = self.trans.checkDuplicate(srcDataFrame, normalDBDataFrame)
            dfNormalCDRNewRecord = dfNormalCDRND.filter("newOrDupl == 'New'")
            dfNormalCDRDuplicate = dfNormalCDRND.filter("newOrDupl == 'Duplicate'")
            self.logger.info("***** generating data for normal cdr - completed *****")
            normalcdr_count = dfNormalCDRNewRecord.groupBy('filename').agg(py_function.count('batch_id').alias('newrec_dm_count').cast(IntegerType()))
            normalcdr_dupl_count = dfNormalCDRDuplicate.groupBy('filename').agg(py_function.count('batch_id').alias('newrec_duplicate_count').cast(IntegerType()))
            return dfNormalCDRNewRecord, dfNormalCDRDuplicate, normalcdr_count, normalcdr_dupl_count
        except Exception as ex:
            self.logger.error("Failed to compute Normal CDR data : {error}".format(error=ex))

    def writetoDataMart(self, sparkSession: SparkSession, srcDataframe: DataFrame, batchStatusDF : (DataFrame, None), tgtColmns=[]):
        try:
            self.logger.info("***** started writing to data mart *****")
            df = srcDataframe.select(*tgtColmns)
            tempTbl = '_'.join(['df_temp', self.property.get("normalcdrtbl")])
            srcDataframe.createGlobalTempView(tempTbl)
            self.redshiftprop.writeToRedshift(df, self.property.get("database"), self.property.get("normalcdrtbl"), tempTbl)
            self.logger.info("***** started writing to data mart - completed *****")
            self.logger.info("***** dropping temp view : {tempTbl}*****".format(tempTbl=tempTbl))
            sparkSession.catalog.dropGlobalTempView(tempTbl)
            self.logger.info("***** dropped temp view : {tempTbl} *****".format(tempTbl=tempTbl))
            if batchStatusDF:
                batch_end_dt = datetime.now()
                batchStatusDF.withColumn('batch_end_dt', py_function.lit(batch_end_dt))
        except Exception as ex:
            self.logger.error("Failed to write data in data mart : {error}".format(error=ex))

    def writetoDuplicateCDR(self, sparkSession: SparkSession, dataframe: DataFrame, tgtColmns=[]):
        try:
            self.logger.info("***** started writing to duplicate mart *****")
            df = dataframe.select(*tgtColmns)
            tempTbl = '_'.join(['df_temp', self.property.get("duplicatecdrtbl")])
            dataframe.createGlobalTempView(tempTbl)
            self.redshiftprop.writeToRedshift(df, self.property.get("database"), self.property.get("duplicatecdrtbl"), tempTbl)
            self.logger.info("***** started writing to duplicate mart - completed *****")
            self.logger.info("***** dropping temp view : {tempTbl}*****".format(tempTbl=tempTbl))
            sparkSession.catalog.dropGlobalTempView(tempTbl)
            self.logger.info("***** dropped temp view : {tempTbl} *****".format(tempTbl=tempTbl))
        except Exception as ex:
            self.logger.error("Failed to write data in duplicate mart : {error}".format(error=ex))

    def writetoLateCDR(self, sparkSession: SparkSession, srcDataframe: DataFrame, batchStatusDF: DataFrame, tgtColmns=[]):
        try:
            self.logger.info("***** started writing to late mart *****")
            df = srcDataframe.select(*tgtColmns)
            tempTbl = '_'.join(['df_temp', self.property.get("latecdrtbl")])
            srcDataframe.createGlobalTempView(tempTbl)
            self.redshiftprop.writeToRedshift(df, self.property.get("database"), self.property.get("latecdrtbl"), tempTbl)
            self.logger.info("***** started writing to late mart - completed *****")
            self.logger.info("***** dropping temp view : {tempTbl}*****".format(tempTbl=tempTbl))
            sparkSession.catalog.dropGlobalTempView(tempTbl)
            self.logger.info("***** dropped temp view : {tempTbl} *****".format(tempTbl=tempTbl))
        except Exception as ex:
            self.logger.error("Failed to write data in late mart : {error}".format(error=ex))

    def writeBatchFileStatus(self, sparkSession: SparkSession, dataframe : DataFrame, batch_id):
        try:
            self.logger.info("Writing batch status metadata")
            self.redshiftprop.updateLogBatchFiles(sparkSession, dataframe, batch_id)
            self.logger.info("Writing batch status metadata - completed")
        except Exception as ex:
            self.logger.error("Failed to write batch status metadata: {error}".format(error=ex))
