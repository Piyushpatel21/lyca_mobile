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
from awsUtils.SecretManager import get_secret


class JSONBuilder:

    def __init__(self, filePath, connfile):
        self.filePath = filePath
        self.connfile = connfile
        self.prop = JsonProcessor.processJsonProperties(self.filePath)
        self.servicetype = JsonProcessor.processRedshiftProp("redshift", connfile)

    def getSourceFilePath(self):
        """ :return source file path in s3"""
        return self.prop['module_prop']['sourceFilePath']

    def getDatabase(self):
        """ :return database name"""
        return self.prop['module_prop']['database']

    def gettbl(self):
        """ :return normal cdr table name"""
        return self.prop['module_prop']['tbl']

    def getLogDB(self):
        """ :return duplicate cdr table name"""
        return self.prop['module_prop']['logdb']

    def getBatchFileTbl(self):
        """ :return duplicate cdr table name"""
        return self.prop['module_prop']['batchfiletbl']

    def getBatchStatusTbl(self):
        """ :return duplicate cdr table name"""
        return self.prop['module_prop']['batchstatustbl']

    def getUsername(self):
        """ :return username of db"""
        return self.servicetype['servicetypeObj'].get('username')

    def getPassword(self):
        """ :return password of db"""
        return self.servicetype['servicetypeObj'].get('password')

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

    def getRedshiftSecrets(self):
        """ :return redshift secret """
        return self.servicetype['servicetypeObj'].get('redshiftsecret')

    def getRegion(self):
        """ :return region name """
        return self.servicetype['servicetypeObj'].get('region')

    def getAppPrpperty(self):
        """ :return return all properties"""
        sourceFilePath = self.getSourceFilePath()
        tbl = self.gettbl()
        database = self.getDatabase()
        logdb = self.getLogDB()
        batchfiletbl = self.getBatchFileTbl()
        batchstatustbl = self.getBatchStatusTbl()
        return {
            "sourceFilePath": sourceFilePath,
            "tbl": tbl,
            "database": database,
            "logdb": logdb,
            "batchfiletbl": batchfiletbl,
            "batchstatustbl": batchstatustbl
        }

    def getConnPrpperty(self):
        return_obj = {}

        user = self.getUsername()
        password = self.getPassword()
        host = self.getHost()
        port = self.getPort()
        tmpdir = self.getTmpdir()
        domain = self.getDomain()
        redshiftsecret = self.getRedshiftSecrets()
        region = self.getRegion()

        if user and password:
            return_obj["user"] = user
            return_obj["password"] = password
        elif redshiftsecret and region:
            secrets = get_secret(redshiftsecret, region)
            return_obj["user"] = secrets["username"]
            return_obj["password"] = secrets["password"]
        else:
            raise Exception("Unable to get the credential for redshift from connecction properties.")
        return_obj.update({
            "host": host,
            "port": port,
            "tmpdir": tmpdir,
            "domain": domain
        })

        return return_obj
