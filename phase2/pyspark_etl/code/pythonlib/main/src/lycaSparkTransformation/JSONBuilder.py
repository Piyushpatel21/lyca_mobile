########################################################################
# description     : Building application properties                    #
# author          : Naren K(narendra.kumar@cloudwick.com),             #
#                   Tejveer Singh(tejveer.singh@cloudwick.com)         #
#                   Shubhajit Saha(shubhajit.saha@cloudwick.com)       #
# contributor     :                                                    #
# version         : 1.0                                                #
# notes           :                                                    #
########################################################################

from commonUtils.JsonProcessor import JsonProcessor


class JSONBuilder:

    def __init__(self, module, submodule, filePath, connfile):
        self.module = module
        self.submodule = submodule
        self.filePath = filePath
        self.connfile = connfile
        self.prop = JsonProcessor.processJsonProperties(self.module, self.submodule, self.filePath)
        self.servicetype = JsonProcessor.processRedshiftProp("redshift", connfile)

    def getModule(self):
        """ :return module name ex: rrbs"""
        return self.prop['module_prop']['module']

    def getSubModule(self):
        """ :return sub module name ex: sms, gprs etc"""
        return self.prop['module_prop']['subModule']

    def getAppName(self):
        """ :return application name for spark session"""
        return self.prop['module_prop']['module'] + " - " + self.prop['module_prop']['subModule']

    def getSourceFilePath(self):
        """ :return source file path in s3"""
        return self.prop['module_prop']['sourceFilePath']

    def getSrcSchemaPath(self):
        """ :return JSON src schema file path"""
        return self.prop['module_prop']['srcSchemaPath']

    def getTgtSchemaPath(self):
        """ :return JSON tgt schema file path"""
        return self.prop['module_prop']['tgtSchemaPath']

    def getDateColumn(self):
        """ :return return date column which is used in dataset to compare with Late or Normal CDR"""
        return self.prop['module_prop']['dateColumn']

    def getFormattedDateColumn(self):
        """ :return formatted column name for date column"""
        return self.prop['module_prop']['formattedDateColumn']

    def getIntegerDateColumn(self):
        """ :return numeric column name for date column"""
        return self.prop['module_prop']['integerDateColumn']

    def getNormalCdrFrq(self):
        """ :return load daily or monthely for normal data"""
        return self.prop['module_prop']['normalcdrfrq']

    def getNumofDayorMnthNormal(self):
        """ :return num of days for normal cdr to check duplicate"""
        return self.prop['module_prop']['numofdayormnthnormal']

    def getLatecdrFrq(self):
        """ :return num of days for late cdr to check duplicate"""
        return self.prop['module_prop']['latecdrfrq']

    def getNumofDayorMnthLate(self):
        """ :return load daily or monthely for normal data"""
        return self.prop['module_prop']['numofdayormnthlate']

    def getDatabase(self):
        """ :return load daily or monthely for normal data"""
        return self.prop['module_prop']['database']

    def getNormalcdrtbl(self):
        """ :return load daily or monthely for normal data"""
        return self.prop['module_prop']['normalcdrtbl']

    def getLatecdrtbl(self):
        """ :return load daily or monthely for normal data"""
        return self.prop['module_prop']['latecdrtbl']

    def getDuplicatecdrtbl(self):
        """ :return load daily or monthely for normal data"""
        return self.prop['module_prop']['duplicatecdrtbl']

    def getUsername(self):
        """ :return load daily or monthely for normal data"""
        return self.servicetype['servicetypeObj']['username']

    def getPassword(self):
        """ :return load daily or monthely for normal data"""
        return self.servicetype['servicetypeObj']['password']

    def getHost(self):
        """ :return load daily or monthely for normal data"""
        return self.servicetype['servicetypeObj']['host']

    def getPort(self):
        """ :return load daily or monthely for normal data"""
        return self.servicetype['servicetypeObj']['port']

    def getTmpdir(self):
        """ :return load daily or monthely for normal data"""
        return self.servicetype['servicetypeObj']['tmpdir']

    def getDomain(self):
        """ :return load daily or monthely for normal data"""
        return self.servicetype['servicetypeObj']['domain']


    def getAppPrpperty(self):
        """ :return return all properties"""
        module = self.getModule()
        subModule = self.getSubModule()
        appname = self.getAppName()
        sourceFilePath = self.getSourceFilePath()
        srcSchemaPath = self.getSrcSchemaPath()
        tgtSchemaPath = self.getTgtSchemaPath()
        dateColumn = self.getDateColumn()
        formattedDateColumn = self.getFormattedDateColumn()
        integerDateColumn = self.getIntegerDateColumn()
        normalcdrfrq = self.getNormalCdrFrq()
        numofdayormnthnormal = self.getNumofDayorMnthNormal()
        latecdrfrq = self.getLatecdrFrq()
        numofdayormnthlate = self.getNumofDayorMnthLate()
        domain = self.getDomain()
        normalcdrtbl = self.getNormalcdrtbl()
        latecdrtbl = self.getLatecdrtbl()
        duplicatecdrtbl = self.getDuplicatecdrtbl()
        database = self.getDatabase()
        return {
            "module": module,
            "subModule": subModule,
            "appname": appname,
            "sourceFilePath": sourceFilePath,
            "srcSchemaPath": srcSchemaPath,
            "tgtSchemaPath": tgtSchemaPath,
            "dateColumn": dateColumn,
            "formattedDateColumn": formattedDateColumn,
            "integerDateColumn": integerDateColumn,
            "normalcdrfrq": normalcdrfrq,
            "numofdayormnthnormal": numofdayormnthnormal,
            "latecdrfrq": latecdrfrq,
            "numofdayormnthlate": numofdayormnthlate,
            "domain": domain,
            "duplicatecdrtbl": duplicatecdrtbl,
            "latecdrtbl": latecdrtbl,
            "normalcdrtbl": normalcdrtbl,
            "database": database
        }

    def getConnPrpperty(self):
        user = self.getUsername()
        password = self.getPassword()
        host = self.getHost()
        port = self.getPort()
        tmpdir = self.getTmpdir()
        domain = self.getDomain()
        return {
            "user": user,
            "password": password,
            "host": host,
            "port": port,
            "tmpdir": tmpdir,
            "domain": domain
        }