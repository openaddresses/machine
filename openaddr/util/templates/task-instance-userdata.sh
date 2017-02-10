#!/bin/bash -ex

# Tell SNS all about it
function notify_sns
{{
    if [ {aws_sns_arn} ]; then
        aws --region {aws_region} sns publish --topic-arn {aws_sns_arn} --subject 'Test Subject' --message 'And this is the test message.'
    fi
}}

notify_sns {message_starting}

# Bail out with a log message
function shutdown_with_log
{{
    if [ $1 = 0 ]; then
        notify_sns {message_complete}
    else
        notify_sns {message_failed}
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
sudo -u ubuntu git checkout FETCH_HEAD

chef/run.sh prereqs
aws s3 cp s3://{bucket}/config/databag-4.json chef/data/local.json
chef/run.sh {role}

# Run the actual command
LC_ALL="C.UTF-8" {command} && shutdown_with_log 0 || shutdown_with_log 1 2>&1
