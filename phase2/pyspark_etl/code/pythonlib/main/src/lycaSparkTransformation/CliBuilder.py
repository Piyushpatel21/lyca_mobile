########################################################################
# description     : Building application properties                    #
# author          : Naren K(narendra.kumar@cloudwick.com),             #
#                   Tejveer Singh (tejveer.singh@cloudwick.com)        #
#                   Shubhajit Saha (shubhajit.saha@cloudwick.com)      #
# contributor     :                                                    #
# version         : 1.0                                                #
# notes           :                                                    #
########################################################################

from commonUtils.CommandLineProcessor import CommandLineProcessor


class CliBuilder:

    def __init__(self, module, submodule, filePath):
        self.module = module
        self.submodule = submodule
        self.filePath = filePath
        self.prop = CommandLineProcessor.processCLIArguments(self.module, self.submodule, self.filePath)

    def getModule(self):
        """ :return module name ex: rrbs"""
        return self.prop['module_prop']['module']

    def getSubModule(self):
        """ :return sub module name ex: sms, gprs etc"""
        return self.prop['module_prop']['sub_module']

    def getAppName(self):
        """ :return application name for spark session"""
        return self.prop['module_prop']['module'] + " - " + self.prop['module_prop']['sub_module']

    def getSourceFilePath(self):
        """ :return source file path in s3"""
        return self.prop['module_prop']['source_file_path']

    def getSchemaPath(self):
        """ :return JSON schema file path"""
        return self.prop['module_prop']['schema_path']

    def getDateColumn(self):
        """ :return return date column which is used in dataset to compare with Late or Normal CDR"""
        return self.prop['module_prop']['dateColumn']

    def getFormattedDateColumn(self):
        """ :return formatted column name for date column"""
        return self.prop['module_prop']['formattedDateColumn']

    def getIntegerDateColumn(self):
        """ :return numeric column name for date column"""
        return self.prop['module_prop']['integerDateColumn']

    def getMnthlyOrdaily(self):
        """ :return load daily or monthely"""
        return self.prop['module_prop']['mnthOrdaily']

    def getNoOfdaysOrMonth(self):
        """ :return no of days or month for dataset comparision"""
        return self.prop['module_prop']['noOfdaysOrMonth']

    def getPrpperty(self):
        """ :return return all properties"""
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
            "integerDateColumn": integerDateColumn,
            "mnthOrdaily": mnthOrdaily,
            "noOfdaysOrMonth": noOfdaysOrMonth
        }
