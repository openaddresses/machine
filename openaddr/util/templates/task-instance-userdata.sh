#!/bin/sh -x
apt-get update -y

# (Re)install machine.
cd /home/ubuntu/machine
sudo -u ubuntu git fetch origin {version}
sudo -u ubuntu git rebase FETCH_HEAD
chef/run.sh {role}

LC_ALL="C.UTF-8" {command} 2>&1

shutdown -h now
