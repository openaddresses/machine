#!/bin/sh -ex
apt-get update -y
apt-get install -y git htop curl

# (Re)install machine.
cd /home/ubuntu/machine
sudo -u ubuntu git fetch origin 2.x
sudo -u ubuntu git rebase FETCH_HEAD
chef/run.sh collector

cat /etc/openaddr-collector.conf
shutdown -h now
