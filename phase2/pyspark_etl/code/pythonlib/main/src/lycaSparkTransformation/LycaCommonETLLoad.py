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
from lycaSparkTransformation.TransformActionChain import TransformActionChain
from lycaSparkTransformation.SparkSessionBuilder import SparkSessionBuilder


class LycaCommonETLLoad:
    """:parameter - Taking input as module
       :parameter sub-module
       :parameter application property file path"""
    def __init__(self):
        pass


module = 'rrbs'
subModule = 'sms'
filePath = os.path.abspath('../../../../config/app_module_level_properties.json')
tf = LycaCommonETLLoad()
sparkSessionBuild = SparkSessionBuilder().sparkSessionBuild()
sparkSession = sparkSessionBuild.get("sparkSession")
logger = sparkSessionBuild.get("logger")
transformactionchain = TransformActionChain(logger, module, subModule, filePath)
propColumns = transformactionchain.srcSchema()
file_list = ["/sample.csv"]
run_date = 20200420
duplicateData, lateUnique, normalUnique = transformactionchain.getSourceData(sparkSession, propColumns.get("srcSchema"), propColumns.get("checkSumColumns"), file_list, run_date)
normalDB, lateDB = transformactionchain.getDbDuplicate(sparkSession)
normalNew, normalDuplicate = transformactionchain.getLateCDR(normalUnique, normalDB)
lateNew, lateDuplicate = transformactionchain.getNormalCDR(lateUnique, lateDB)
outputCDR = [duplicateData, normalNew, normalDuplicate, lateNew, lateNew, lateDuplicate]
normalDuplicate.show(150, False)
transformactionchain.dfWrite(lateNew, run_date, 'dataMart', 'normalDB.csv', propColumns.get("tgtSchema"))
transformactionchain.dfWrite(duplicateData, run_date, 'duplicateModel', 'duplicate.csv', propColumns.get("tgtSchema"))
transformactionchain.dfWrite(lateNew, run_date, 'lateCDR', 'late.csv', propColumns.get("tgtSchema"))
transformactionchain.dfWrite(lateDuplicate, run_date, 'duplicateModel', 'duplicate.csv', propColumns.get("tgtSchema"))
transformactionchain.dfWrite(normalNew, run_date, 'dataMart', 'normalDB.csv', propColumns.get("tgtSchema"))
transformactionchain.dfWrite(normalDuplicate, run_date, 'duplicateModel', 'duplicate.csv', propColumns.get("tgtSchema"))