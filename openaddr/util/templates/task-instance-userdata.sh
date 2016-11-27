#!/bin/bash -ex

# Bail out with a log message
function shutdown_with_log
{{
    mkdir /tmp/task
    gzip -c /var/log/cloud-init-output.log > /tmp/task/cloud-init-output.log.gz
    echo {command} > /tmp/task/command
    echo $1 > /tmp/task/status
    
    AWS_ACCESS_KEY_ID={access_key} AWS_SECRET_ACCESS_KEY={secret_key} \
        aws s3 cp /tmp/task s3://{bucket}/{log_prefix}/ --recursive --acl private
    
    shutdown -h now
}}

# Bail out when the timer reaches zero
( sleep {lifespan}; shutdown_with_log 9 ) &

# (Re)install machine.
cd /home/ubuntu/machine
sudo -u ubuntu git fetch origin {version}
sudo -u ubuntu git rebase FETCH_HEAD

apt-get update -y
chef/run.sh {role}

# Run the actual command
LC_ALL="C.UTF-8" {command} 2>&1
shutdown_with_log $?
