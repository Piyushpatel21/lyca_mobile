########################################################################
# description     : Building application level param and calling       #
# author          : Naren K(narendra.kumar@cloudwick.com),             #
#                   Tejveer Singh(tejveer.singh@cloudwick.com)         #
#                   Shubhajit Saha(shubhajit.saha@cloudwick.com)       #
# contributor     :                                                    #
# version         : 1.0                                                #
# notes           :                                                    #
########################################################################

import sys
from datetime import datetime, timedelta
from lycaSparkTransformation.TransformActionChain import TransformActionChain
from lycaSparkTransformation.SparkSessionBuilder import SparkSessionBuilder


class LycaCommonETLLoad:
    """:parameter - Taking input as module
       :parameter sub-module
       :parameter application property file path"""

    def __init__(self, module, submodule, configfile, connfile, master, run_date=None, batchID= None):
        self.batchID = batchID
        self.module = module
        self.submodule = submodule
        self.configfile = configfile
        self.connfile = connfile
        self.master = master
        self.run_date = run_date

    def parseArguments(self):
        return {
            "run_date": self.run_date,
            "module": self.module,
            "submodule": self.submodule,
            "configfile": self.configfile,
            "connfile": self.connfile,
            "master": self.master,
            "batchID": self.batchID
        }

    def hourRounder(self, t):
        # Rounds to nearest hour by adding a timedelta hour if minute >= 30
        return (t.replace(second=0, microsecond=0, minute=0, hour=t.hour)
                + timedelta(hours=t.minute // 30))

    def getTimeInterval(self, now):
        return now + timedelta(hours=6)


def start_execution(args):
    lycaETL = LycaCommonETLLoad(args.get('module'), args.get('submodule'), args.get('configfile'), args.get('connfile'),
                                args.get('master'), args.get('run_date'), args.get('batchID'))
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
    tf = TransformActionChain(sparkSession, logger, args.get('module'), args.get('submodule'), configfile, connfile, run_date, prevDate)
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
                                .join(latecdr_dupl_count, on='filename', how='left_outer')
        tf.writetoDataMart(normalNew, propColumns.get("tgtSchema"))
        tf.writetoLateCDR(lateNew, propColumns.get("tgtSchema"))
        tf.writetoDuplicateCDR(lateDuplicate, propColumns.get("tgtSchema"))
        tf.writetoDuplicateCDR(duplicateData, propColumns.get("tgtSchema"))
        tf.writetoDuplicateCDR(normalDuplicate, propColumns.get("tgtSchema"))
        tf.writetoDataMart(lateNew, propColumns.get("tgtSchema"))
        tf.writeBatchFileStatus(dfmetadata, batch_id)
        logger.error("ETL processing completed for batch - {batch_id}".format(batch_id=batch_id))
        tf.writeBatchStatus(batch_id, "Complete")
        tf.writeBatchFileStatus(dfmetadata, batch_id)
    except Exception as ex:
        logger.error("ETL processing failed for batch - {batch_id} : {error}".format(error=ex, batch_id=batch_id))
        tf.writeBatchStatus(batch_id, "Failed")
