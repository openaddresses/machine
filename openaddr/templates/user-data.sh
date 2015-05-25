#!/bin/sh -ex
apt-get update -y
while [ ! -f /tmp/machine.tar.gz ]; do sleep 10; done

echo 'extracting' > /var/run/machine-state.txt
mkdir /tmp/machine
tar -C /tmp/machine -xzf /tmp/machine.tar.gz

echo 'installing' > /var/run/machine-state.txt
/tmp/machine/ec2/swap.sh
/tmp/machine/chef/run.sh batchmode

echo 'processing' > /var/run/machine-state.txt
openaddr-process -a {access_key} -s {secret_key} -l log.txt /var/opt/openaddresses/sources {bucketname}

echo 'terminating' > /var/run/machine-state.txt
shutdown -h now
