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


class CommandLineProcessor:

    @staticmethod
    def json_parser(filepath):
        """:parameter filepath - Json file path
           :return streaming byte of input file"""
        with open(filepath, 'r') as paramFile:
            return json.load(paramFile)

    @staticmethod
    def processCLIArguments(module, sub_module, filepath):
        """:parameter module - read property for a particular module
           :parameter sub_module - read property for a particular sub-module
           :parameter filepath - path of JSON
           :return JSON object"""
        idx = 0
        try:
            data = CommandLineProcessor.json_parser(filepath)
            for obj in data:
                if obj["module"] == module and obj["sub_module"] == sub_module:
                    return {
                        "module_prop": obj
                    }
                else:
                    continue
        except (OSError, IOError, ValueError) as ex:
            print("Param source file not found : Path - " + ex)
