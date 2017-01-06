#!/bin/sh -e

DIR=`dirname $0`
MAJOR="`cut -f1 -d. $DIR/../openaddr/VERSION`.x"
WORKERS="CI Workers $MAJOR"
ALARM="No Active Workers"

# boolean value from the alarm that is ON when nobody's at work
BUSY=`aws cloudwatch describe-alarms --alarm-names "$ALARM" | jq '.MetricAlarms[0].StateValue != "ALARM"'`

if [ $BUSY = 'true' ]; then
    echo "Boo, found busy active workers in alarm $ALARM."
    exit 1
fi

echo "- Yay, found nobody busy in alarm $ALARM."

for INSTANCE in `aws autoscaling describe-auto-scaling-groups --auto-scaling-group-name "$WORKERS" | jq '.AutoScalingGroups[0].Instances[].InstanceId' | tr -d '"'`; do
    echo "Terminating instance $INSTANCE from group $WORKERS..."
    aws ec2 terminate-instances --instance-ids $INSTANCE > /dev/null
done
