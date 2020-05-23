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
import os
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
    batch_from = ''
    batch_to = ''
    if not (args.get('run_date') and args.get('batchID')):
        prevDate = datetime.now() + timedelta(days=-1)
        run_date = prevDate.date().strftime('%Y%m%d')
        batch_from = lycaETL.hourRounder(prevDate)
        batch_to = lycaETL.getTimeInterval(batch_from)
    else:
        run_date = args.get('run_date')
    appname = args.get('module') + '-' + args.get('submodule')
    configfile = args.get('configfile')
    connfile = args.get('connfile')
    sparkSessionBuild = SparkSessionBuilder(args.get('master'), appname).sparkSessionBuild()
    sparkSession = sparkSessionBuild.get("sparkSession")
    logger = sparkSessionBuild.get("logger")
    tf = TransformActionChain(logger, args.get('module'), args.get('submodule'), configfile, connfile, run_date, batch_from, batch_to)
    if not (args.get('run_date') and args.get('batchID')):
        batch_id = tf.getBatchID(sparkSession)
    else:
        batch_id = args.get('batchID')
    if not batch_id:
        logger.error("Batch ID not available for current timestamp : batch_from={batch_from}, batch_to={batch_to}".format(batch_from=batch_from, batch_to=batch_to))
        sys.exit(1)
    logger.info("Running application for : run_date={run_date}, batch_id={batch_id}, batch_from={batch_from}, "
                "batch_to={batch_to} "
                .format(batch_id=batch_id, run_date=run_date,batch_from=batch_from, batch_to=batch_to))
    propColumns = tf.srcSchema()
    duplicateData, lateUnique, normalUnique = tf.getSourceData(sparkSession, batch_id, propColumns.get("srcSchema"), propColumns.get("checkSumColumns"))
    lateUnique.show(20, False)
    normalDB, lateDB = tf.getDbDuplicate(sparkSession)
    normalNew, normalDuplicate = tf.getNormalCDR(normalUnique, normalDB)
    lateNew, lateDuplicate = tf.getLateCDR(lateUnique, lateDB)
    normalNewcnt = normalNew.count()
    lateNewCnt = lateNew.count()
    lateDuplicateCnt = lateDuplicate.count()
    duplicateDataCnt = duplicateData.count()
    normalDuplicateCnt = normalDuplicate.count()
    print("we are processing : normalNew={normalNew}, lateNew={lateNew}, lateDuplicate={lateDuplicate}, duplicateData={duplicateData}, normalDuplicate={normalDuplicate}"
          .format(normalNew=normalNewcnt, lateNew=lateNewCnt, lateDuplicate=lateDuplicateCnt, duplicateData=duplicateDataCnt, normalDuplicate=normalDuplicateCnt))
    tf.writetoDataMart(sparkSession, normalNew, propColumns.get("tgtSchema"))
    tf.writetoLateCDR(sparkSession, lateNew, propColumns.get("tgtSchema"))
    tf.writetoDuplicateCDR(sparkSession, lateDuplicate, propColumns.get("tgtSchema"))
    tf.writetoDuplicateCDR(sparkSession, duplicateData, propColumns.get("tgtSchema"))
    tf.writetoDuplicateCDR(sparkSession, normalDuplicate, propColumns.get("tgtSchema"))
    tf.writetoDataMart(sparkSession, lateNew, propColumns.get("tgtSchema"))
#
#
# def parseArguments():
#     parser = argparse.ArgumentParser()
#     parser.add_argument('--run_date', help='run date required for trigger pipeline')
#     parser.add_argument('--batchID', help='run date required for trigger pipeline')
#     parser.add_argument('--module', help='module name required to process data')
#     parser.add_argument('--submodule', help='submodule name required to process data')
#     parser.add_argument('--configfile', help='application module level config file path')
#     parser.add_argument('--connfile', help='connection config file path')
#     parser.add_argument('--master', help='session for glue')
#     known_arguments, unknown_arguments = parser.parse_known_args()
#     arguments = vars(known_arguments)
#     if arguments:
#         if not (arguments.get('module') and arguments.get('submodule')):
#             print("--run_date --module, --submodule required for trigger pipeline")
#             sys.exit(1)
#     return arguments
#
#
# if __name__ == '__main__':
#     args = parseArguments()
#     start_execution(args)
