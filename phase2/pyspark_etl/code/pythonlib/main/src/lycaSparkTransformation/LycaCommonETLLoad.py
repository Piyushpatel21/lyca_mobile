########################################################################
# description     : Building application level param and calling       #
# author          : Naren K(narendra.kumar@cloudwick.com),             #
#                   Tejveer Singh(tejveer.singh@cloudwick.com)         #
#                   Shubhajit Saha(shubhajit.saha@cloudwick.com)       #
# contributor     :                                                    #
# version         : 1.0                                                #
# notes           :                                                    #
########################################################################

import os
import argparse
from lycaSparkTransformation.TransformActionChain import TransformActionChain
from lycaSparkTransformation.SparkSessionBuilder import SparkSessionBuilder
import sys


class LycaCommonETLLoad:
    """:parameter - Taking input as module
       :parameter sub-module
       :parameter application property file path"""

    def parseArguments(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--run_date', help='run date required for trigger pipeline')
        parser.add_argument('--module', help='module name required to process data')
        parser.add_argument('--submodule', help='submodule name required to process data')
        parser.add_argument('--configfile', help='application module level config file path')
        parser.add_argument('--connfile', help='connection config file path')
        known_arguments, unknown_arguments = parser.parse_known_args()
        arguments = vars(known_arguments)
        if arguments:
            if not (arguments.get('run_date') and arguments.get('module') and arguments.get('submodule')):
                print("--run_date --module, --submodule required for trigger pipeline")
                sys.exit(1)
        return arguments


lycaETL = LycaCommonETLLoad()
args = lycaETL.parseArguments()
configfile = os.path.abspath(args.get('configfile'))
connfile = os.path.abspath(args.get('connfile'))
sparkSessionBuild = SparkSessionBuilder().sparkSessionBuild()
sparkSession = sparkSessionBuild.get("sparkSession")
logger = sparkSessionBuild.get("logger")
batchid = 101
tf = TransformActionChain(logger, args.get('module'), args.get('submodule'), configfile, connfile, batchid)
propColumns = tf.srcSchema()
duplicateData, lateUnique, normalUnique = tf.getSourceData(sparkSession, propColumns.get("srcSchema"), propColumns.get("checkSumColumns"), args.get('run_date'))
normalDB, lateDB = tf.getDbDuplicate(sparkSession, args.get('run_date'))
normalNew, normalDuplicate = tf.getNormalCDR(normalUnique, normalDB)
lateNew, lateDuplicate = tf.getLateCDR(lateUnique, lateDB)
outputCDR = [duplicateData, normalNew, normalDuplicate, lateNew, lateNew, lateDuplicate]
tf.writetoDataMart(normalNew, propColumns.get("tgtSchema"))
tf.writetoDataMart(lateNew, propColumns.get("tgtSchema"))
tf.writetoLateCDR(lateNew, propColumns.get("tgtSchema"))
tf.writetoDuplicateCDR(lateDuplicate, propColumns.get("tgtSchema"))
tf.writetoDuplicateCDR(duplicateData, propColumns.get("tgtSchema"))
tf.writetoDuplicateCDR(normalDuplicate, propColumns.get("tgtSchema"))
