#!/bin/sh -e

DIR=`dirname $0`
MAJOR="`cut -f1 -d. $DIR/../openaddr/VERSION`.x"
WEBHOOKS="CI Webhooks $MAJOR"
ELB="default-openaddresses-io"

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
