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
sparkSessionBuild = SparkSessionBuilder().sparkSessionBuild()
sparkSession = sparkSessionBuild.get("sparkSession")
logger = sparkSessionBuild.get("logger")
tf = TransformActionChain(logger, args.get('module'), args.get('submodule'), configfile)
propColumns = tf.srcSchema()
file_list = ["/sample.csv"]
duplicateData, lateUnique, normalUnique = tf.getSourceData(sparkSession, propColumns.get("srcSchema"), propColumns.get("checkSumColumns"), file_list, args.get('run_date'))
normalDB, lateDB = tf.getDbDuplicate(sparkSession, args.get('run_date'))
normalNew, normalDuplicate = tf.getNormalCDR(normalUnique, normalDB)
lateNew, lateDuplicate = tf.getLateCDR(lateUnique, lateDB)
outputCDR = [duplicateData, normalNew, normalDuplicate, lateNew, lateNew, lateDuplicate]
normalDuplicate.show(150, False)
tf.dfWrite(lateNew, args.get('run_date'), 'dataMart', 'normalDB.csv', propColumns.get("tgtSchema"))
tf.dfWrite(duplicateData, args.get('run_date'), 'duplicateModel', 'duplicate.csv', propColumns.get("tgtSchema"))
tf.dfWrite(lateNew, args.get('run_date'), 'lateCDR', 'late.csv', propColumns.get("tgtSchema"))
tf.dfWrite(lateDuplicate, args.get('run_date'), 'duplicateModel', 'duplicate.csv', propColumns.get("tgtSchema"))
tf.dfWrite(normalNew, args.get('run_date'), 'dataMart', 'normalDB.csv', propColumns.get("tgtSchema"))
tf.dfWrite(normalDuplicate, args.get('run_date'), 'duplicateModel', 'duplicate.csv', propColumns.get("tgtSchema"))