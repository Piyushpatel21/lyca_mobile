from commonUtils.CommandLineProcessor import CommandLineProcessor


class CliBuilder:

    def __init__(self, module, submodule, filePath):
        self.module = module
        self.submodule = submodule
        self.filePath = filePath
        self.prop = CommandLineProcessor.processCLIArguments(self.module, self.submodule, self.filePath)

    def getModule(self):
        return self.prop['module_prop']['module']

    def getSubModule(self):
        return self.prop['module_prop']['sub_module']

    def getAppName(self):
        return self.prop['module_prop']['module'] + " - " + self.prop['module_prop']['sub_module']

    def getSourceFilePath(self):
        return self.prop['module_prop']['source_file_path']

    def getSchemaPath(self):
        return self.prop['module_prop']['schema_path']

    def getInputDateColumn(self):
        return self.prop['module_prop']['inputdateColumn']

    def getOutputDateColumn(self):
        return self.prop['module_prop']['outputdateColumn']

    def getMnthlyOrdaily(self):
        return self.prop['module_prop']['mnthOrdaily']

    def getNoOfdaysOrMonth(self):
        return self.prop['module_prop']['noOfdaysOrMonth']


    def getPrpperty(self):
        module = self.getModule()
        subModule = self.getSubModule()
        appname = self.getAppName()
        sourceFilePath = self.getSourceFilePath()
        schemaPath = self.getSchemaPath()
        inputdateColumn = self.getInputDateColumn()
        outputdateColumn = self.getOutputDateColumn()
        mnthOrdaily = self.getMnthlyOrdaily()
        noOfdaysOrMonth = self.getNoOfdaysOrMonth()
        return {
            "module": module,
            "subModule": subModule,
            "appname": appname,
            "sourceFilePath": sourceFilePath,
            "schemaPath": schemaPath,
            "inputdateColumn": inputdateColumn,
            "outputdateColumn": outputdateColumn,
            "mnthOrdaily": mnthOrdaily,
            "noOfdaysOrMonth": noOfdaysOrMonth
        }