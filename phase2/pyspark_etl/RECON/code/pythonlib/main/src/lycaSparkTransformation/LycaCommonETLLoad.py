########################################################################
# description     : Building application level param and calling       #
# author          : Naren K(narendra.kumar@cloudwick.com),             #
#                   Tejveer Singh(tejveer.singh@cloudwick.com)         #
#                   Shubhajit Saha(shubhajit.saha@cloudwick.com)       #
# contributor     :                                                    #
# version         : 1.0                                                #
# notes           :                                                    #
########################################################################
import argparse
import sys
from datetime import datetime, timedelta
from lycaSparkTransformation.TransformActionChain import TransformActionChain
from lycaSparkTransformation.SparkSessionBuilder import SparkSessionBuilder


class LycaCommonETLLoad:
    """:parameter - Taking input as module
       :parameter sub-module
       :parameter application property file path"""

    def __init__(self, configfile, connfile, master, code_bucket, encoding, run_date=None, batchID= None):
        self.batchID = batchID
        self.configfile = configfile
        self.connfile = connfile
        self.master = master
        self.run_date = run_date
        self.code_bucket = code_bucket
        self.encoding = encoding

    def parseArguments(self):
        return {
            "run_date": self.run_date,
            "configfile": self.configfile,
            "connfile": self.connfile,
            "master": self.master,
            "batchID": self.batchID,
            "code_bucket": self.code_bucket,
            "encoding": self.encoding
        }


def start_execution(args):
    lycaETL = LycaCommonETLLoad(configfile=args.get('configfile'), connfile=args.get('connfile'),
                                master=args.get('master'), code_bucket=args.get('code_bucket'),
                                encoding=args.get('encoding'), run_date=args.get('run_date'),
                                batchID=args.get('batchID'))
    args = lycaETL.parseArguments()
    prevDate = datetime.now() + timedelta(days=-1)
    if not (args.get('run_date') and args.get('batchID')):
        run_date = prevDate.date().strftime('%Y%m%d')
    else:
        run_date = args.get('run_date')
    appname = 'recon' + '-' + str(run_date)
    configfile = args.get('configfile')
    connfile = args.get('connfile')
    sparkSessionBuild = SparkSessionBuilder(args.get('master'), appname).sparkSessionBuild()
    sparkSession = sparkSessionBuild.get("sparkSession")
    logger = sparkSessionBuild.get("logger")
    tf = TransformActionChain(sparkSession, logger, args.get('module'), args.get('submodule'),
                              configfile, connfile, run_date, prevDate, args.get('code_bucket'), args.get('encoding'))
    if not (args.get('run_date') and args.get('batchID')):
        batch_id = tf.getBatchID()
    else:
        batch_id = args.get('batchID')
    if not batch_id:
        logger.error("Batch ID not available for current timestamp : prevDate={prevDate}".format(prevDate=prevDate))
        sys.exit(1)
    logger.info("Running application for : run_date={run_date}, batch_id={batch_id}, prevDate={prevDate}"
                .format(batch_id=batch_id, run_date=run_date, prevDate=prevDate))
    try:
        df_raw, dfmetadata = tf.getSourceData(batch_id)
        df_source = df_raw.select('recon_file_name', 's3_prefix', 'source_file_name',
                                                      'source_rec_count', 'batch_id', 'created_date')
        df_raw.show(20, False)
        dfmetadata.show(20, False)
        tf.writeBatchFileStatus(dfmetadata, batch_id)
        tf.writetoDataMart(df_source)
        logger.info("ETL processing completed for batch - {batch_id}".format(batch_id=batch_id))
        tf.writeBatchStatus(batch_id, "Complete")
    except Exception as ex:
        logger.error("ETL processing failed for batch - {batch_id} : {error}".format(error=ex, batch_id=batch_id))
        tf.writeBatchStatus(batch_id, "Failed")
        raise Exception(ex)
