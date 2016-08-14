#!/bin/sh -x
apt-get update -y
apt-get install -y git htop curl

# (Re)install machine.
cd /home/ubuntu/machine
sudo -u ubuntu git fetch origin {version}
sudo -u ubuntu git rebase FETCH_HEAD
chef/run.sh {role}

{command} 2>&1

shutdown -h now
