import json


class CommandLineProcessor:

    @staticmethod
    def json_parser(filepath):
        with open(filepath, 'r') as paramFile:
            return json.load(paramFile)

    @staticmethod
    def processCLIArguments(module, sub_module, filepath):
        try:
            data = CommandLineProcessor.json_parser(filepath)
            for obj in data:
                if obj["module"] == module and obj["sub_module"] == sub_module:
                    return {
                        "module_prop": obj
                    }
                    break
                else:
                    continue
        except (OSError, IOError) as ex:
            print("Param source file not found : Path - " + filepath)
        except ValueError:
            print('Decoding JSON has failed')
