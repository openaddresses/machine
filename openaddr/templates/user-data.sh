#!/bin/sh -ex
apt-get update -y
apt-get install -y git

echo 'cloning' > /var/www/html/state.txt
git clone -b {branch} {repository} /tmp/machine

echo 'installing' > /var/www/html/state.txt
/tmp/machine/ec2/swap.sh
/tmp/machine/chef/run.sh batchmode

echo 'processing' > /var/www/html/state.txt
openaddr-process -a {access_key} -s {secret_key} -l log.txt /var/opt/openaddresses/sources {bucketname}

echo 'terminating' > /var/www/html/state.txt
shutdown -h now
