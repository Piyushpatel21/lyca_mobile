import os

from lycaSparkTransformation.CliBuilder import CliBuilder
from lycaSparkTransformation.TransformActionChain import TransformActionChain


class TransformAction:
    def __init__(self, module, subModule, filePath):
        self.module = module
        self.submodule = subModule
        self.filePath = filePath
        self.cli = CliBuilder(module, subModule, filePath)

    def buildCLI(self):
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

files =['SMS_2019090200.cdr']
transformactionchain = TransformActionChain(module, subModule, ap, sourceFilePath, schemaPath, files)

