# Glue Job Run

## To execute job

* We need to pass lambda name in shared account that will execute job in dev

```shell script
LAMBDA_NAME='platform-lambda-manage-glue-rManageGlue-1GYED7HMI04Q0'

python run_glue_job.py -n ${LAMBDA_NAME} -j lyca_etl_rrbs_sms
```

## To execute job with extra arguments

```shell script
LAMBDA_NAME='platform-lambda-manage-glue-rManageGlue-1GYED7HMI04Q0'

python run_glue_job.py -n ${LAMBDA_NAME} -j lyca_etl_rrbs_sms --run_date 20200419 --batch_id 104
```