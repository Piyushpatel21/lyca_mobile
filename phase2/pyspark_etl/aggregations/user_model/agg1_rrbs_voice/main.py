"""
Entry point of RRBS Voice Aggregation
"""


import sys
import argparse
from code.aggregations import start_execution


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', type=str, default='', nargs='?', help='Start date of aggregation in format yyyyMMdd inclusive of value.')
    parser.add_argument('--end_date', type=str, default='', nargs='?', help='End date of aggregation in format yyyyMMdd inclusive of value.')
    parser.add_argument('--module', help='module name required to process data')
    parser.add_argument('--sub_module', help='submodule name required to process data')
    parser.add_argument('--configfile', help='application module level config file path')
    parser.add_argument('--connfile', help='connection config file path')
    parser.add_argument('--master', help='session for glue')
    parser.add_argument('--code_bucket', help='Name of the code bucket')
    known_arguments, unknown_arguments = parser.parse_known_args()
    arguments = vars(known_arguments)
    if arguments:
        if not (arguments.get('module') and arguments.get('sub_module')):
            print("--module, --sub_module required for trigger pipeline")
            sys.exit(1)
        if bool(arguments.get('start_date')) ^ bool(arguments.get('end_date')):
            print("Both --start_date and --end_date should exist conjointly.")
            sys.exit(1)
    return arguments


if __name__ == '__main__':
    args = parse_arguments()
    start_execution(args)
