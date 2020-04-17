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

    def getDateColumn(self):
        return self.prop['module_prop']['dateColumn']

    def getFormattedDateColumn(self):
        return self.prop['module_prop']['formattedDateColumn']

    def getIntegerDateColumn(self):
        return self.prop['module_prop']['integerDateColumn']

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
        dateColumn = self.getDateColumn()
        formattedDateColumn = self.getFormattedDateColumn()
        integerDateColumn = self.getIntegerDateColumn()
        mnthOrdaily = self.getMnthlyOrdaily()
        noOfdaysOrMonth = self.getNoOfdaysOrMonth()
        return {
            "module": module,
            "subModule": subModule,
            "appname": appname,
            "sourceFilePath": sourceFilePath,
            "schemaPath": schemaPath,
            "dateColumn": dateColumn,
            "formattedDateColumn": formattedDateColumn,
            "integerDateColumn" : integerDateColumn,
            "mnthOrdaily": mnthOrdaily,
            "noOfdaysOrMonth": noOfdaysOrMonth
        }