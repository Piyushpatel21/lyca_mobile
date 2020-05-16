########################################################################
# description     : Building application level param and calling       #
# author          : Naren K(narendra.kumar@cloudwick.com),             #
#                   Tejveer Singh(tejveer.singh@cloudwick.com)         #
#                   Shubhajit Saha(shubhajit.saha@cloudwick.com)       #
# contributor     :                                                    #
# version         : 1.0                                                #
# notes           :                                                    #
########################################################################

from lycaSparkTransformation.TransformActionChain import TransformActionChain
from lycaSparkTransformation.SparkSessionBuilder import SparkSessionBuilder


class LycaCommonETLLoad:
    """:parameter - Taking input as module
       :parameter sub-module
       :parameter application property file path"""

    def __init__(self, run_date, module, submodule, configfile, connfile, master):
        self.run_date = run_date
        self.module = module
        self.submodule = submodule
        self.configfile = configfile
        self.connfile = connfile
        self.master = master

    def parseArguments(self):
        return {
            "run_date": self.run_date,
            "module": self.module,
            "submodule": self.submodule,
            "configfile": self.configfile,
            "connfile": self.connfile,
            "master": self.master
        }


def start_execution(args):
    lycaETL = LycaCommonETLLoad(args.get('run_date'), args.get('module'), args.get('submodule'), args.get('configfile'), args.get('connfile'), args.get('master'))
    args = lycaETL.parseArguments()
    appname = args.get('module') + '-' + args.get('submodule')
    configfile = args.get('configfile')
    connfile = args.get('connfile')
    sparkSessionBuild = SparkSessionBuilder(args.get('master'), appname).sparkSessionBuild()
    sparkSession = sparkSessionBuild.get("sparkSession")
    logger = sparkSessionBuild.get("logger")
    batchid = 101
    tf = TransformActionChain(logger, args.get('module'), args.get('submodule'), configfile, connfile, batchid, args.get('run_date'))
    propColumns = tf.srcSchema()
    duplicateData, lateUnique, normalUnique = tf.getSourceData(sparkSession, propColumns.get("srcSchema"), propColumns.get("checkSumColumns"))
    normalDB, lateDB = tf.getDbDuplicate(sparkSession)
    normalNew, normalDuplicate = tf.getNormalCDR(normalUnique, normalDB)
    lateNew, lateDuplicate = tf.getLateCDR(lateUnique, lateDB)
    outputCDR = [duplicateData, normalNew, normalDuplicate, lateNew, lateNew, lateDuplicate]
    cnt = normalNew.count()
    logger.info("Writing new record in redshift" + str(cnt))
    tf.writetoDataMart(normalNew, propColumns.get("tgtSchema"))
    logger.info("====== Writing new record in redshift completed =======" + str(cnt))
    tf.writetoDataMart(lateNew, propColumns.get("tgtSchema"))
    tf.writetoLateCDR(lateNew, propColumns.get("tgtSchema"))
    tf.writetoDuplicateCDR(lateDuplicate, propColumns.get("tgtSchema"))
    tf.writetoDuplicateCDR(duplicateData, propColumns.get("tgtSchema"))
    tf.writetoDuplicateCDR(normalDuplicate, propColumns.get("tgtSchema"))
