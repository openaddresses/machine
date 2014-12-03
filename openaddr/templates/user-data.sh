#!/bin/sh -ex
apt-get update -y
apt-get install -y git apache2

echo 'cloning' > /var/www/html/state.txt
git clone https://github.com/openaddresses/machine /tmp/machine

echo 'installing' > /var/www/html/state.txt
/tmp/machine/chef/run.sh

echo 'processing' > /var/www/html/state.txt
openaddr-process -a {access_key} -s {secret_key} -l log.txt {bucketname}

echo 'terminating' > /var/www/html/state.txt
openaddr-ec2-kill {ec2_access_key} {ec2_secret_key}
