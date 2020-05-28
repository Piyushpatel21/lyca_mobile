"""
Script to create, update glue job
"""

import argparse
import json
import boto3
from botocore.exceptions import ClientError

JOB_PARAMETERS = {
    'Name': None,
    'Description': None,
    'Role': None,
    'MaxConcurrentRuns': 1,
    'ETLJobType': "glueetl",
    'ScriptLocation': None,
    'PythonVersion': "3",
    'DefaultArguments': None,
    'Connections': None,
    'MaxRetries': 1,
    'Timeout': 2880,
    'MaxCapacity': 2,
    'GlueVersion': "1.0",
    'WorkerType': None,
    'NumberOfWorkers': None,
    'Tags': None
}

JOB_PARAMETERS_VALIDATIONS = {
    'MaxConcurrentRuns': lambda x: 0 < int(x) <= 50,
    'ETLJobType': lambda x: x in ['glueetl', 'pythonshell'],
    'PythonVersion': lambda x: int(x) in [2, 3],
    'Timeout': lambda x: 0 < int(x) <= 2880,
    'MaxCapacity': lambda x: 2 <= float(x) <= 100,
    'GlueVersion': lambda x: x in ['0.9', '1.0']
}

MANDATORY_JOB_PARAMETERS = ['Name', 'Description', 'Role', 'ScriptLocation']


def build_job_args(default_args, new_job_args):
    """
    Validates parameters and create final arguments dict

    :param default_args: existing of default arguments
    :param new_job_args: new arguments
    :return:
    """
    default_args.update(new_job_args)

    for k, validation_func in JOB_PARAMETERS_VALIDATIONS.items():
        if not validation_func(default_args[k]):
            raise Exception("Failed to validate value for key: {key}".format(key=k))

    final_args = {
        'Name': default_args.get('Name'),
        'Description': default_args.get('Description'),
        'Role': default_args.get('Role'),
        'ExecutionProperty': {
            'MaxConcurrentRuns': default_args.get('MaxConcurrentRuns')
        },
        'Command': {
            'Name': default_args.get('ETLJobType'),
            'ScriptLocation': default_args.get('ScriptLocation'),
            'PythonVersion': default_args.get('PythonVersion')
        },
        'DefaultArguments': default_args.get('DefaultArguments'),
        'MaxRetries': default_args.get('MaxRetries'),
        'Timeout': default_args.get('Timeout'),
        'Tags': default_args.get('Tags'),
        'GlueVersion': default_args.get('GlueVersion')
    }
    if default_args.get('Connections'):
        final_args.update({
            'Connections': {
                'Connections': list(default_args.get('Connections'))
            }
        })
    if default_args.get('WorkerType') and default_args.get('NumberOfWorkers'):
        final_args.update({
            'WorkerType': default_args.get('WorkerType'),
            'NumberOfWorkers': default_args.get('NumberOfWorkers')
        })
    elif default_args.get('MaxCapacity'):
        final_args.update({
            'MaxCapacity': default_args.get('MaxCapacity')
        })
    else:
        raise Exception("Workload type need to defined which can be either via WorkerType "
                        "and NumberOfWorkers or MaxCapacity")

    return final_args


def error_response(msg):
    """
    Structure error message

    :param msg:
    :return:
    """
    return {
        "exitcode": 1,
        "message": msg
    }


def create_job(glue_client, job_args: dict):
    """
    Creates Glue job

    :param glue_client: Glue Client
    :param job_args: arguments for the job
    :return:
    """
    missing_params = MANDATORY_JOB_PARAMETERS - job_args.keys()
    if missing_params:
        raise Exception("Following Mandatory parameters are missing: {missing}".format(missing=missing_params))
    final_args = build_job_args(JOB_PARAMETERS, job_args)
    response = glue_client.create_job(**final_args)
    return response


def update_job(glue_client, job_name, job_args: dict):
    """
    Updates the existing job

    :param glue_client: Glue Client
    :param job_name: Name of the job
    :param job_args: arguments for the job
    :return:
    """
    job_details = get_job(glue_client, job_name)
    if 'exitcode' in job_details and job_details['exitcode'] == 1:
        response = job_details
    elif job_details:
        existing_job = job_details['Job']
        existing_job_args = {}
        for key in JOB_PARAMETERS.keys():
            if key == 'MaxConcurrentRuns':
                existing_job_args['MaxConcurrentRuns'] = existing_job['ExecutionProperty']['MaxConcurrentRuns']
            elif key == 'ETLJobType':
                existing_job_args['ETLJobType'] = existing_job["Command"]["Name"]
            elif key == 'ScriptLocation':
                existing_job_args['ScriptLocation'] = existing_job['Command']['ScriptLocation']
            elif key == 'PythonVersion':
                existing_job_args['PythonVersion'] = existing_job['Command']['PythonVersion']
            elif key == 'Connections':
                if 'Connections' in existing_job:
                    existing_job_args['Connections'] = existing_job['Connections']['Connections']
            else:
                if key in existing_job:
                    existing_job_args[key] = existing_job[key]
                else:
                    existing_job_args[key] = JOB_PARAMETERS[key]
        final_args = build_job_args(existing_job_args, job_args)
        del final_args['Name']
        del final_args['Tags']

        # Updating glue job
        response = glue_client.update_job(
            JobName=job_name,
            JobUpdate=final_args
        )

    else:
        response = error_response("Job with name {job} not found.".format(job=job_name))
    return response


def delete_job(glue_client, job_name):
    """
    Deletes the glue job

    :param glue_client: Glue Client
    :param job_name: Name of the job
    :return:
    """

    response = glue_client.delete_job(
        JobName=job_name
    )

    return response


def get_job(glue_client, job_name):
    """
    Get the glue job details

    :param glue_client: Glue Client
    :param job_name: Name of the job
    :return:
    """

    response = glue_client.get_job(
        JobName=job_name
    )
    return response


def json_to_dict(json_file):
    """
    Convert json file to dictionary

    :param json_file: json file location, must be local
    :return:
    """
    with open(json_file) as f:
        dict_data = json.load(f)
    return dict_data


def manage_command(command, job_name=None, config_file=None, configs=None):
    """
    Entry point for util execution.

    :param command: (Required) Type of operation, allowed values: get_job, create_job, update_job, delete_job
    :param job_name: Name of the job. Required if command is get_job, update_job, delete_job
    :param config_file: json file containing configs
    :param configs: dictionary with configs
    :return:
    """

    if command not in ['get_job', 'create_job', 'update_job', 'delete_job']:
        raise Exception('Command {cmd} is not available, can be one of following: get_job, create_job, '
                        'update_job, delete_job'.format(cmd=command))

    if command in ['get_job', 'delete_job', 'update_job'] and not job_name:
        raise Exception("-j <job-name> is mandatory when command is {cmd}".format(cmd=command))
    elif command == 'create_job' and not (config_file or configs):
        raise Exception("-f <config-file> or --configs <configs dict> is mandatory when command is create_job")

    try:

        glue_client = boto3.client('glue')
        if command == "get_job":
            response = get_job(glue_client, job_name)
        elif command == 'create_job':
            if config_file:
                configs = json_to_dict(config_file)
            response = create_job(glue_client, configs)
        elif command == 'update_job':
            if config_file:
                configs = json_to_dict(config_file)
            response = update_job(glue_client, job_name, configs)
        elif command == 'delete_job':
            response = delete_job(glue_client, job_name)
        else:
            response = None
    except ClientError as cli_err:
        if cli_err.response['Error']['Code'] == 'ParamValidationError':
            response = error_response("Failed to validate parameters with error {err}".format(err=cli_err))
        elif cli_err.response['Error']['Code'] == 'EntityNotFoundException':
            response = error_response('Job with name {j_name} does not exists.'.format(j_name=job_name))
        else:
            response = error_response("Failed to perform operation with error {err}.".format(err=cli_err))
    except Exception as ex:
        response = error_response("Failed to perform operation with error {err}.".format(err=ex))

    return response


def parse_arguments():
    """
    Parse the input arguments

    :return:
    """
    ap = argparse.ArgumentParser()

    ap.add_argument('-c', '--command', required=True, help='Can be one of following: get_job, create_job, '
                                                           'update_job, delete_job')
    ap.add_argument('-j', '--job_name', help='Name of the job')
    ap.add_argument('-f', '--config_file', help='Config file for glue job')
    ap.add_argument('--configs', help='Configs as dictionary which is compliment to config_file.')
    known_arguments, unknown_arguments = ap.parse_known_args()
    arguments = vars(known_arguments)
    return arguments


if __name__ == '__main__':
    args = parse_arguments()
    response = manage_command(**args)
    print(response)
