#!/bin/sh -ex
apt-get update -y
apt-get install -y git htop curl

# (Re)install machine.
cd /home/ubuntu/machine
sudo -u ubuntu git fetch origin {version}
sudo -u ubuntu git rebase FETCH_HEAD
chef/run.sh collector

honcho -e /etc/openaddr-collector.conf run openaddr-collect-extracts \
    --owner {owner} --repository {repository} --bucket {bucket} \
    --database-url {database_url} --access-key {access_key} \
    --secret-key {secret_key} --sns-arn {sns_arn} 2>&1

shutdown -h now
