#! /bin/bash
set -eux

mkdir -p ~/.aws
cat <<EOF > creds.tpl
[mis_lycamobile]
aws_access_key_id = {{ AWS_ACCESS_KEY_ID }}
aws_secret_access_key = {{ AWS_SECRET_ACCESS_KEY }}
EOF
cat <<EOF > config.tpl
[profile mis_code_commit]
role_arn = arn:aws:iam::869841577715:role/OrganizationAccountCodeCommit
source_profile = mis_lycamobile
region = {{ AWS_REGION }}
EOF

envtpl  < creds.tpl > ~/.aws/credentials
envtpl  < config.tpl > ~/.aws/config

git config --global credential.helper '!aws --profile mis_code_commit codecommit credential-helper $@'
git config --global credential.UseHttpPath true
git config --global user.email "git@cloudwick.com"
git config --global user.name "Git CI/CD"

git remote remove code_commit || printf "code_commit does not exist"
git remote add code_commit https://git-codecommit.eu-west-2.amazonaws.com/v1/repos/lycamobile-etl-movements

git remote -v

git push code_commit ${CI_COMMIT_REF_NAME} || sleep 10000
