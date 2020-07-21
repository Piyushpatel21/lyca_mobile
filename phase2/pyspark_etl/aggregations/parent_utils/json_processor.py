########################################################################
# description     : processing JSON config files.                      #
# author          : Naren K(narendra.kumar@cloudwick.com),             #
#                   Tejveer Singh(tejveer.singh@cloudwick.com)         #
#                   Shubhajit Saha(shubhajit.saha@cloudwick.com)
#                   Bhavin Tandel(bhavin.tandel@cloudwick.com)
# contributor     :                                                    #
# version         : 1.0                                                #
# notes           :                                                    #
########################################################################

import json


class JsonProcessor:

    @staticmethod
    def json_parser(file_content):
        """:parameter file_content - Json file content
           :return streaming byte of input file"""

        return json.loads(file_content)

    @staticmethod
    def process_app_properties(module, sub_module, file_content):
        """:parameter module - read property for a particular module
           :parameter sub_module - read property for a particular sub-module
           :parameter file_content - file content of JSON
           :return JSON object"""

        try:
            data = JsonProcessor.json_parser(file_content)
            for obj in data:
                if obj["module"] == module and obj["subModule"] == sub_module:
                    return obj
                else:
                    continue
        except (OSError, IOError, ValueError) as ex:
            print("Failed to process application prop file : Path - " + ex)
            raise Exception(ex)

    @staticmethod
    def process_conn_properties(file_content, servicetype='redshift'):
        try:
            data = JsonProcessor.json_parser(file_content)
            for obj in data:
                if obj["servicetype"] == servicetype:
                    return obj
                else:
                    continue
        except (OSError, IOError, ValueError) as ex:
            print("Failed to process connection file : - " + str(ex))
            raise Exception(ex)

    @staticmethod
    def process_schema(file_content):
        try:
            data = JsonProcessor.json_parser(file_content)
            return data
        except (OSError, IOError, ValueError) as ex:
            print("Failed to process schema file : - {err}".format(err=ex))
            raise Exception(ex)
