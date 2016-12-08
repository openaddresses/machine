#!/bin/bash -ex

# Tell Slack all about it
function notify_slack
{{
    if [ {slack_url} ]; then
        echo $1 | curl -s -o /dev/null -X POST -d @- {slack_url} || true
    fi
}}

notify_slack {message_starting}

# Bail out with a log message
function shutdown_with_log
{{
    if [ $1 = 0 ]; then
        notify_slack {message_complete}
    else
        notify_slack {message_failed}
    fi
    
    mkdir /tmp/task
    gzip -c /var/log/cloud-init-output.log > /tmp/task/cloud-init-output.log.gz
    echo {command} > /tmp/task/command
    echo $1 > /tmp/task/status
    
    aws s3 cp /tmp/task s3://{bucket}/{log_prefix}/ --recursive --acl private
    
    shutdown -h now
}}

# Bail out when the timer reaches zero
( sleep {lifespan}; shutdown_with_log 2 ) &

# (Re)install machine.
cd /home/ubuntu/machine
sudo -u ubuntu git fetch origin {version}
sudo -u ubuntu git checkout {version}

chef/run.sh prereqs
aws s3 cp s3://{bucket}/config/databag-3.json chef/data/local.json
chef/run.sh {role}

# Run the actual command
LC_ALL="C.UTF-8" {command} && shutdown_with_log 0 || shutdown_with_log 1 2>&1
