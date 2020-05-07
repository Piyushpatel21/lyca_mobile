########################################################################
# description     : processing JSON config files.                      #
# author          : Naren K(narendra.kumar@cloudwick.com),             #
#                   Tejveer Singh(tejveer.singh@cloudwick.com)         #
#                   Shubhajit Saha(shubhajit.saha@cloudwick.com)       #
# contributor     :                                                    #
# version         : 1.0                                                #
# notes           :                                                    #
########################################################################

import json


class JsonProcessor:

    @staticmethod
    def json_parser(filepath):
        """:parameter filepath - Json file path
           :return streaming byte of input file"""
        with open(filepath, 'r') as paramFile:
            return json.load(paramFile)

    @staticmethod
    def processJsonProperties(module, sub_module, filepath):
        """:parameter module - read property for a particular module
           :parameter sub_module - read property for a particular sub-module
           :parameter filepath - path of JSON
           :return JSON object"""
        try:
            data = JsonProcessor.json_parser(filepath)
            for obj in data:
                if obj["module"] == module and obj["subModule"] == sub_module:
                    return {
                        "module_prop": obj
                    }
                else:
                    continue
        except (OSError, IOError, ValueError) as ex:
            print("failed to process application prop file : Path - " + ex)

    @staticmethod
    def processRedshiftProp(serivetype, filepath):
        try:
            data = JsonProcessor.json_parser(filepath)
            for obj in data:
                if obj["servicetype"] == serivetype:
                    return {
                        "servicetypeObj": obj
                    }
                else:
                    continue
        except (OSError, IOError, ValueError) as ex:
            print("failed to process connection file : - " + ex)
