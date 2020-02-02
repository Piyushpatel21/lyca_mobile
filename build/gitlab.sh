#! /bin/bash
set -eu
mkdir ~/.aws
cat <<EOF > creds.tpl
[mis_lycamobile]                                                
aws_access_key_id = {{ AWS_ACCESS_KEY_ID }}        
aws_secret_access_key = {{ AWS_SECRET_ACCESS_KEY }}
EOF
cat <<EOF > ~/.aws/config
[profile mis_code_commit]
role_arn = arn:aws:iam::869841577715:role/OrganizationAccountCodeCommit
source_profile = mis_lycamobile
EOF

envtpl  < creds.tpl > ~/.aws/credentials

git config --global credential.helper '!aws --profile mis_code_commit codecommit credential-helper $@'
git config --global credential.UseHttpPath true
git config --global user.email "git@cloudwick.com"
git config --global user.name "Git CI/CD"

export branch=$(git rev-parse --abbrev-ref HEAD)
git remote add code_comit https://git-codecommit.eu-west-2.amazonaws.com/v1/repos/lyca-etl-movements
git remote -v
git push code_commit $branch
