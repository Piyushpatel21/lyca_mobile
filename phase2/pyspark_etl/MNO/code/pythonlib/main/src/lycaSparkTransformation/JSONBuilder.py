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
        print(self.prop)
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

    def getIntegerDateColumn(self):
        """ :return numeric column name for date column"""
        return self.prop['module_prop']['integerDateColumn']

    def getNormalCdrFrq(self):
        """ :return normal cdr frequency"""
        return self.prop['module_prop']['normalcdrfrq']

    def getNumofDayorMnthNormal(self):
        """ :return date range for normal cdr"""
        return self.prop['module_prop']['numofdayormnthnormal']

    def getLatecdrFrq(self):
        """ :return late cdr frequency"""
        return self.prop['module_prop']['latecdrfrq']

    def getNumofDayorMnthLate(self):
        """ :return date range for late cdr"""
        return self.prop['module_prop']['numofdayormnthlate']

    def getDatabase(self):
        """ :return database name"""
        return self.prop['module_prop']['database']

    def getNormalcdrtbl(self):
        """ :return normal cdr table name"""
        return self.prop['module_prop']['normalcdrtbl']

    def getLatecdrtbl(self):
        """ :return late cdr table name"""
        return self.prop['module_prop']['latecdrtbl']

    def getDuplicatecdrtbl(self):
        """ :return duplicate cdr table name"""
        return self.prop['module_prop']['duplicatecdrtbl']

    def getUsername(self):
        """ :return username of db"""
        return self.servicetype['servicetypeObj']['username']

    def getPassword(self):
        """ :return password of db"""
        return self.servicetype['servicetypeObj']['password']

    def getHost(self):
        """ :return host of database"""
        return self.servicetype['servicetypeObj']['host']

    def getPort(self):
        """ :return port for db"""
        return self.servicetype['servicetypeObj']['port']

    def getTmpdir(self):
        """ :return temp dir"""
        return self.servicetype['servicetypeObj']['tmpdir']

    def getDomain(self):
        """ :return domain name"""
        return self.servicetype['servicetypeObj']['domain']

    def getAppPrpperty(self):
        """ :return return all properties"""
        module = self.getModule()
        subModule = self.getSubModule()
        appname = self.getAppName()
        sourceFilePath = self.getSourceFilePath()
        srcSchemaPath = self.getSrcSchemaPath()
        tgtSchemaPath = self.getTgtSchemaPath()
        integerDateColumn = self.getIntegerDateColumn()
        normalcdrfrq = self.getNormalCdrFrq()
        numofdayormnthnormal = self.getNumofDayorMnthNormal()
        latecdrfrq = self.getLatecdrFrq()
        numofdayormnthlate = self.getNumofDayorMnthLate()
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
            "integerDateColumn": integerDateColumn,
            "normalcdrfrq": normalcdrfrq,
            "numofdayormnthnormal": numofdayormnthnormal,
            "latecdrfrq": latecdrfrq,
            "numofdayormnthlate": numofdayormnthlate,
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