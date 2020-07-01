"""
Invokes the lambda to run the glue job
"""

import boto3
import argparse
import json


def invoke_lambda(lambda_name, job_name, run_args, region=None, dryrun=False):
    """
    Invokes the lambda to run glue
    """
    try:
        if region:
            lambda_client = boto3.client('lambda', region_name=region)
        else:
            lambda_client = boto3.client('lambda')

        if dryrun:
            invocation_type = 'DryRun'
        else:
            invocation_type = 'RequestResponse'

        payload = {
            'job_name': job_name,
            'command': 'run_job',
            'run_args': run_args
        }

        response = lambda_client.invoke(
            FunctionName=lambda_name,
            InvocationType=invocation_type,
            Payload=json.dumps(payload).encode('utf-8')
        )
    except Exception as ex:
        print("Error occurred: {err}".format(err=ex))
        response = "Error occurred: {err}".format(err=ex)

    return response


def parse_arguments():
    """
    Parse the input arguments

    :return:
    """
    ap = argparse.ArgumentParser()

    ap.add_argument('-n', '--lambda_name', required=True, help='Name of the lambda')
    ap.add_argument('-j', '--job_name', required=True, help='Name of the job')
    ap.add_argument('--run_date', type=str, help='Rundate of the module')
    ap.add_argument('--batch_id', type=str, help='Batch id of the module')
    ap.add_argument('--source_file_path', help='Source file path for one time load')
    ap.add_argument('--region', help='Region of aws to run the command in.')
    ap.add_argument('--dryrun', action='store_true')
    known_arguments, unknown_arguments = ap.parse_known_args()
    arguments = vars(known_arguments)
    u_arguments = unknown_arguments
    return arguments, u_arguments


if __name__ == '__main__':
    args, u_args = parse_arguments()
    print(args)
    print(u_args)
    run_args ={}

    if args.get('run_date'):
        run_args['--run_date'] = args.get('run_date')
    if args.get('batch_id'):
        run_args['--batchID'] = args.get('batch_id')
    if args.get('source_file_path'):
        run_args['--source_file_path'] = args.get('source_file_path')

    response = invoke_lambda(args.get('lambda_name'), args.get('job_name'),
                             run_args, region=args.get('region'), dryrun=args.get('dryrun'))
    print(response['Payload'].read())
