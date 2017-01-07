#!/bin/sh -e

DIR=`dirname $0`
MAJOR="`cut -f1 -d. $DIR/../openaddr/VERSION`.x"
WORKERS="CI Workers $MAJOR"
CRONTAB="CI Crontab $MAJOR"
WEBHOOKS="CI Webhooks $MAJOR"
ELB="default-openaddresses-io"
ALARM="No Active Workers"

CYCLE_WORKERS=false
CYCLE_CRONTAB=false
CYCLE_WEBHOOKS=false

if [ $# -eq 0 ]; then
    echo 'Cycling worker, crontab, and webhooks instances'
    CYCLE_WORKERS=true
    CYCLE_CRONTAB=true
    CYCLE_WEBHOOKS=true
else
    for WORD in $@; do
        if [ $WORD = 'workers' ]; then
            echo 'Cycling worker instances'
            CYCLE_WORKERS=true
        elif [ $WORD = 'crontab' ]; then
            echo 'Cycling crontab instances'
            CYCLE_CRONTAB=true
        elif [ $WORD = 'webhooks' ]; then
            echo 'Cycling webhooks instances'
            CYCLE_WEBHOOKS=true
        else
            echo "Usage: $0 [{workers|crontab|webhooks}, ...]"
            exit 1
        fi
    done
fi

#
# Cycle worker instances
#

if $CYCLE_WORKERS; then
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
fi

#
# Cycle crontab instances
#

if $CYCLE_CRONTAB; then
    for INSTANCE in `aws autoscaling describe-auto-scaling-groups --auto-scaling-group-name "$CRONTAB" | jq '.AutoScalingGroups[0].Instances[].InstanceId' | tr -d '"'`; do
        echo "Terminating instance $INSTANCE from group $CRONTAB..."
        aws ec2 terminate-instances --instance-ids $INSTANCE > /dev/null
    done
fi

#
# Cycle webhooks instances
#

if $CYCLE_WEBHOOKS; then
    echo "Setting capacity of group $WEBHOOKS to 2..."
    aws autoscaling set-desired-capacity --desired-capacity 2 --auto-scaling-group-name "$WEBHOOKS"

    echo "Counting number of instances in group $WEBHOOKS..."
    while true; do
        COUNT=`aws autoscaling describe-auto-scaling-groups --auto-scaling-group-name "$WEBHOOKS" | jq '.AutoScalingGroups[0].Instances[].LifecycleState == "InService"' | grep true | wc -l | tr -d '[:space:]'`
    
        # Wait until there are two instances.
        if [ $COUNT -ge 2 ]; then
            echo " - Yay, there are $COUNT instances in group $WEBHOOKS"
            break
        else
            echo " - Boo, there are $COUNT instances in group $WEBHOOKS"
            sleep 30
        fi
    done

    echo "Counting number of instances in load balancer $ELB..."
    while true; do
        COUNT=`aws elb describe-instance-health --load-balancer-name "$ELB" | jq '.InstanceStates[].State == "InService"' | grep true | wc -l | tr -d '[:space:]'`
    
        # Wait until there are two instances.
        if [ $COUNT -ge 2 ]; then
            echo " - Yay, there are $COUNT instances in load balancer $ELB"
            break
        else
            echo " - Boo, there are $COUNT instances in load balancer $ELB"
            sleep 30
        fi
    done

    echo "Setting capacity of group $WEBHOOKS to 1..."
    aws autoscaling set-desired-capacity --desired-capacity 1 --auto-scaling-group-name "$WEBHOOKS"
fi
