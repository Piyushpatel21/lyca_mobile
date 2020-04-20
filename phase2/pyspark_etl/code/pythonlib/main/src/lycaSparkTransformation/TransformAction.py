########################################################################
# description     : Building application level param and calling       #
# author          : Naren K(narendra.kumar@cloudwick.com),             #
#                   Tejveer Singh (tejveer.singh@cloudwick.com)        #
#                   Shubhajit Saha (shubhajit.saha@cloudwick.com)      #
# contributor     :                                                    #
# version         : 1.0                                                #
# notes           :                                                    #
########################################################################

import os

from lycaSparkTransformation.CliBuilder import CliBuilder
from lycaSparkTransformation.TransformActionChain import TransformActionChain


class TransformAction:
    """:parameter - Taking input as module
       :parameter sub-module
       :parameter application property file path"""
    def __init__(self, module, subModule, filePath):
        self.module = module
        self.submodule = subModule
        self.filePath = filePath
        self.cli = CliBuilder(module, subModule, filePath)

    def buildCLI(self):
        """ Building application properties"""
        moduleProp = self.cli.getPrpperty()
        return {
            "prop": moduleProp
        }

module = 'rrbs'
subModule = 'sms'
filePath = os.path.abspath('../../../../config/app_module_level_properties.json')
tf = TransformAction(module, subModule, filePath)

properties = tf.buildCLI()
module = properties["prop"]["module"]
subModule = properties["prop"]["subModule"]
ap = properties["prop"]["appname"]
sourceFilePath = properties["prop"]["sourceFilePath"]
schemaPath = properties["prop"]["schemaPath"]
dateColumn = properties["prop"]["dateColumn"]
formattedDateColumn = properties["prop"]["formattedDateColumn"]
integerDateColumn = properties["prop"]["integerDateColumn"]
mnthOrdaily = properties["prop"]["mnthOrdaily"]
noOfdaysOrMonth = properties["prop"]["noOfdaysOrMonth"]

files =['SMS_2019090200.cdr']
transformactionchain = TransformActionChain(module, subModule, ap, sourceFilePath, schemaPath, files, dateColumn, formattedDateColumn, integerDateColumn, mnthOrdaily, noOfdaysOrMonth)

