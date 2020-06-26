# Stack Deployment Documentation

This document contains the documentation of the stack deployed to AWS prior to CI/CD being set

## Requirements

- aws # pip install aws

## Validation example

```sh
aws cloudformation validate-template --region eu-west-2  --template-body file://platform_s3_buckets_for_each_env.yaml
```

## Deployment example

```sh
aws_profile="mis_profile_name"
aws_account="$AWS_ACCOUNT_ID"
environment=dev
aws cloudformation deploy --template-file deployment/orgs/platform_s3_buckets.yaml \
    --stack-name "platform-s3-buckets-$environment" \
    --parameter-overrides Environment=$environment \
    --profile $aws_profile --region $aws_region
```

## Deployed

```sh
# The AccountIDs
export AWS_MIS_ROOT=419299522998
export AWS_MIS_LOGGING=998673941131
export AWS_MIS_ALERTS=581201638086
export AWS_MIS_NETWORKING=454290209215
export AWS_MIS_SHARED_SERVICES=869841577715
export AWS_MIS_CONTROL_PLANE=217830175691
export AWS_MIS_UK_DEV=311477489434
export AWS_MIS_UK_PROD=255804926358
export AWS_REGION="eu-west-2" # London
country_code="uk"
```

### Datalake uk dev

```sh
# Default configuration required
aws_region="${AWS_REGION}"
aws_profile="mis_lycamobile_dev"
country_code="uk"
aws_account="${AWS_MIS_UK_DEV}"
environment="dev"
```
# File mover

This is a simple Lambda function and accompanying resources to catch files in the landing zone and move them to another directory as defined in the DynamoDB table (mapping with `SourceDir` and `OutputDir` as keys) and suffixed with `yyyy/mm/dd/`.

```
export SFTP_BUCKET="mis-dl-${country_code}-${aws_region}-${aws_account}-${environment}-raw"
export CFN_BUCKET="mis-dl-${country_code}-${aws_region}-${aws_account}-${environment}-cloudform"
export NOTIFICATION_EMAIL="sakshi.agarwal@cloudwick.com"

aws cloudformation package --template-file s3_scripts/file_mover/file_mover.yaml --s3-bucket $CFN_BUCKET \
--output-template-file s3_scripts/file_mover/file_mover_out.yaml \
--profile $aws_profile --region $aws_region

aws cloudformation deploy --template-file s3_scripts/file_mover/file_mover_out.yaml \
--stack-name mis-dl-lambda-file-mover-uk-dev \
--parameter-overrides LandingBucket=$SFTP_BUCKET,NotificationEmailId=$NOTIFICATION_EMAIL \
--profile $aws_profile --region $aws_region --capabilities CAPABILITY_NAMED_IAM

```

### Datalake uk prod

```sh
# Default configuration required
aws_region="${AWS_REGION}"
aws_profile="mis_lycamobile_prod"
country_code="uk"
aws_account="${AWS_MIS_UK_PROD}"
environment="prod"
```
# File mover

This is a simple Lambda function and accompanying resources to catch files in the landing zone and move them to another directory as defined in the DynamoDB table (mapping with `SourceDir` and `OutputDir` as keys) and suffixed with `yyyy/mm/dd/`.

```
export SFTP_BUCKET="mis-dl-${country_code}-${aws_region}-${aws_account}-${environment}-raw"
export CFN_BUCKET="mis-dl-${country_code}-${aws_region}-${aws_account}-${environment}-cloudform"
export NOTIFICATION_EMAIL="sakshi.agarwal@cloudwick.com"

aws cloudformation package --template-file s3_scripts/file_mover/file_mover.yaml --s3-bucket $CFN_BUCKET \
--output-template-file s3_scripts/file_mover/file_mover_out.yaml \
--profile $aws_profile --region $aws_region

aws cloudformation deploy --template-file s3_scripts/file_mover/file_mover_out.yaml \
--stack-name mis-dl-lambda-file-mover-uk-prod \
--parameter-overrides LandingBucket=$SFTP_BUCKET,NotificationEmailId=$NOTIFICATION_EMAIL \
--profile $aws_profile --region $aws_region --capabilities CAPABILITY_NAMED_IAM

```

In addition to this, the DynamoDB table created by the stack should be populated with mappings of SourceDir to OutputDir, and the S3 bucket should be set to send notifications to the SNS topic created by the stack. Both of these can be automated, but in the interests of simplicity this can happen outside of Cloudformation for now.
