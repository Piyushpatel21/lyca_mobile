import argparse
import sys

from lycaSparkTransformation.LycaCommonETLLoad import start_execution


def parseArguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--run_date', help='run date required for trigger pipeline')
    parser.add_argument('--batchID', help='run date required for trigger pipeline')
    parser.add_argument('--module', help='module name required to process data')
    parser.add_argument('--submodule', help='submodule name required to process data')
    parser.add_argument('--configfile', help='application module level config file path')
    parser.add_argument('--connfile', help='connection config file path')
    parser.add_argument('--master', help='session for glue')
    parser.add_argument('--code_bucket', help='Name of the code bucket')
    known_arguments, unknown_arguments = parser.parse_known_args()
    arguments = vars(known_arguments)
    if arguments:
        if not (arguments.get('module') and arguments.get('submodule')):
            print("--run_date --module, --submodule required for trigger pipeline")
            sys.exit(1)
    return arguments


if __name__ == '__main__':
    args = parseArguments()
    print(args)
    start_execution(args)