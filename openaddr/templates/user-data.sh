#!/bin/sh -ex
apt-get update -y
apt-get install -y git apache2

echo 'cloning' > /var/www/html/state.txt
git clone -b ditch-node https://github.com/openaddresses/machine /tmp/machine

echo 'installing' > /var/www/html/state.txt
apt-get install -y build-essential ruby ruby-dev # For now install chef here instead of run.sh,
gem install ohai -v 7.4.0 --no-rdoc --no-ri      # because of 2014-12-01 dependency conflict:
gem install chef --no-rdoc --no-ri               # https://github.com/opscode/chef/issues/2517
/tmp/machine/chef/run.sh

echo 'processing' > /var/www/html/state.txt
openaddr-process -a {access_key} -s {secret_key} -l log.txt {bucketname}

echo 'killing' > /var/www/html/state.txt
python <<KILL

from urllib import urlopen
from boto.ec2 import EC2Connection

instance = urlopen('http://169.254.169.254/latest/meta-data/instance-id').read().strip()
EC2Connection('{ec2_access_key}', '{ec2_secret_key}').terminate_instances(instance)

KILL
