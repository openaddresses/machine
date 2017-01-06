#!/bin/sh -e

DIR=`dirname $0`
MAJOR="`cut -f1 -d. $DIR/../openaddr/VERSION`.x"
CRONTAB="CI Crontab $MAJOR"

for INSTANCE in `aws autoscaling describe-auto-scaling-groups --auto-scaling-group-name "$CRONTAB" | jq '.AutoScalingGroups[0].Instances[].InstanceId' | tr -d '"'`; do
    echo "Terminating instance $INSTANCE from group $CRONTAB..."
    aws ec2 terminate-instances --instance-ids $INSTANCE > /dev/null
done
