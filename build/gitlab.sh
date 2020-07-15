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

mv ~/.aws/credentials{,"-`date`"} || true
mv ~/.aws/config{,"-`date`"} || true

envtpl  < creds.tpl > ~/.aws/credentials
envtpl  < config.tpl > ~/.aws/config

git config --local credential.helper '!aws --profile mis_code_commit codecommit credential-helper $@'
git config --local credential.UseHttpPath true
git config --local user.email "git@cloudwick.com"
git config --local user.name "Git CI/CD"

git remote remove code_commit || printf "code_commit does not exist"
git remote add code_commit https://git-codecommit.eu-west-2.amazonaws.com/v1/repos/lycamobile-etl-movements

git remote -v

git fetch --unshallow || git fetch # gitlab started cloning repositories with shallow clone by default
# git checkout origin/${CI_COMMIT_REF_NAME}
git pull origin ${CI_COMMIT_REF_NAME}
printf "hash: $(git rev-parse HEAD) ci_ref: ${CI_COMMIT_REF_NAME}\n"
git push code_commit HEAD:${CI_COMMIT_REF_NAME}
