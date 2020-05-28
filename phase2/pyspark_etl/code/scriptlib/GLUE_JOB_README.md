# Glue Job Creation


Mandatory config parameters: `'Name', 'Description', 'Role', 'ScriptLocation'`

To add external libs zip and temp directory, json should consist of,

```json
"DefaultArguments": {
    "--extra-py-files": "s3://python_lib_package.zip",
    "--TempDir": "s3://path_to_temp_dir"
  }
```

Template can be found [here](./glue_job_config_template.json)

## Create Glue job

```shell script
python glueUtil.py -c create_job -f glue_job_config_template.json
```

## Get Glue job

```shell script
python glueUtil.py -c get_job -j TestJob2
```

## Update Glue job

```shell script
python glueUtil.py -c update_job -j TestJob2 -f glue_job_config_template.json
```

### Delete Glue job

```shell script
python glueUtil.py -c delete_job -j TestJob2
```
