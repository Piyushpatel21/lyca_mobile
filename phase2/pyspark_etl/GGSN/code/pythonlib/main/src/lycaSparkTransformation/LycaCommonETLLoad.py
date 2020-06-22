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

    def __init__(self, module, submodule, configfile, connfile, master, code_bucket, run_date=None, batchID= None):
        self.batchID = batchID
        self.module = module
        self.submodule = submodule
        self.configfile = configfile
        self.connfile = connfile
        self.master = master
        self.run_date = run_date
        self.code_bucket = code_bucket

    def parseArguments(self):
        return {
            "run_date": self.run_date,
            "module": self.module,
            "submodule": self.submodule,
            "configfile": self.configfile,
            "connfile": self.connfile,
            "master": self.master,
            "batchID": self.batchID,
            "code_bucket": self.code_bucket
        }


def start_execution(args):
    lycaETL = LycaCommonETLLoad(module=args.get('module'), submodule=args.get('submodule'),
                                configfile=args.get('configfile'), connfile=args.get('connfile'),
                                master=args.get('master'), code_bucket=args.get('code_bucket'),
                                run_date=args.get('run_date'), batchID=args.get('batchID'))
    args = lycaETL.parseArguments()
    prevDate = datetime.now() + timedelta(days=-1)
    if not (args.get('run_date') and args.get('batchID')):
        run_date = prevDate.date().strftime('%Y%m%d')
    else:
        run_date = args.get('run_date')
    appname = args.get('module') + '-' + args.get('submodule')
    configfile = args.get('configfile')
    connfile = args.get('connfile')
    sparkSessionBuild = SparkSessionBuilder(args.get('master'), appname).sparkSessionBuild()
    sparkSession = sparkSessionBuild.get("sparkSession")
    logger = sparkSessionBuild.get("logger")
    tf = TransformActionChain(sparkSession, logger, args.get('module'), args.get('submodule'),
                              configfile, connfile, run_date, prevDate, args.get('code_bucket'))
    if not (args.get('run_date') and args.get('batchID')):
        batch_id = tf.getBatchID()
    else:
        batch_id = args.get('batchID')
    if not batch_id:
        logger.error("Batch ID not available for current timestamp : prevDate={prevDate}".format(prevDate=prevDate))
        sys.exit(1)
    logger.info("Running application for : run_date={run_date}, batch_id={batch_id}, prevDate={prevDate}"
                .format(batch_id=batch_id, run_date=run_date, prevDate=prevDate))
    propColumns = tf.srcSchema()
    try:
        duplicateData, lateUnique, normalUnique, recordCount = tf.getSourceData(batch_id, propColumns.get("srcSchema"), propColumns.get("checkSumColumns"))
        normalDB, lateDB,  = tf.getDbDuplicate()
        normalNew, normalDuplicate, normalcdr_count, normalcdr_dupl_count = tf.getNormalCDR(normalUnique, normalDB, batch_id)
        lateNew, lateDuplicate, latecdr_count, latecdr_dupl_count = tf.getLateCDR(lateUnique, lateDB, batch_id)
        dfmetadata = recordCount.join(normalcdr_count, on='filename', how='left_outer') \
                                .join(normalcdr_dupl_count, on='filename', how='left_outer') \
                                .join(latecdr_count, on='filename', how='left_outer') \
                                .join(latecdr_dupl_count, on='filename', how='left_outer') \
                                .withColumnRenamed('filename', 'FILE_NAME') \
                                .na.fill(0)
        # print("We are processing normalNew={normalNew}, lateNew={lateNew}, lateDuplicate={lateDuplicate}, "
        #       "duplicateData={duplicateData}, normalDuplicate={normalDuplicate}, lateNew={lateNew}"
        #       .format(normalNew=normalNew.count(), lateNew=lateNew.count(), lateDuplicate=lateDuplicate.count(),
        #               duplicateData=duplicateData.count(), normalDuplicate=normalDuplicate.count()))
        # dfmetadata.show(20, False)
        tf.writeBatchFileStatus(dfmetadata, batch_id)
        tf.writetoDuplicateCDR(duplicateData, propColumns.get("tgtSchema"))
        tf.writetoDuplicateCDR(lateDuplicate, propColumns.get("tgtSchema"))
        tf.writetoDuplicateCDR(normalDuplicate, propColumns.get("tgtSchema"))
        tf.writetoDataMart(normalNew, propColumns.get("tgtSchema"))
        tf.writetoLateCDR(lateNew, propColumns.get("tgtSchema"))
        tf.writetoDataMart(lateNew, propColumns.get("tgtSchema"))
        logger.info("ETL processing completed for batch - {batch_id}".format(batch_id=batch_id))
        tf.writeBatchStatus(batch_id, "Complete")
    except Exception as ex:
        logger.error("ETL processing failed for batch - {batch_id} : {error}".format(error=ex, batch_id=batch_id))
        tf.writeBatchStatus(batch_id, "Failed")
        raise Exception(ex)
