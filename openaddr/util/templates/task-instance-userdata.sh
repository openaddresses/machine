#!/bin/bash -ex

# Tell SNS all about it
function notify_sns
{{
    if [ {aws_sns_arn} ]; then
        aws --region {aws_region} sns publish --topic-arn {aws_sns_arn} --subject "$1" --message "$1"
    fi
}}

notify_sns 'Starting {command_name}...'

# Bail out with a log message
function shutdown_with_log
{{
    if [ $1 = 0 ]; then
        notify_sns 'Completed {command_name}'
    else
        notify_sns 'Failed {command_name}'
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

# Prepare temp volume, if applicable
if [ -b /dev/xvdb ]; then
    mkfs.ext3 /dev/xvdb
    mount /dev/xvdb /tmp
fi

# (Re)install machine.
docker pull openaddr/machine:{version}
aws s3 cp s3://data.openaddresses.io/config/environment-5.txt /tmp/environment

# Run the actual command
docker run --env-file /tmp/environment --volume /tmp:/tmp --net="host" openaddr/machine:{version} \
    {command} && shutdown_with_log 0 || shutdown_with_log 1 2>&1
