#!/usr/bin/env python3
''' Update EC2 instances in OpenAddresses Webhooks group.

Webhooks is the only continuously-running instance in OpenAddresses, and
responds to external web requests. Upgrading is done with a blue/green deploy
process, with the number of desired instances in the autoscaling group raised
to two and then lowered back to one after a new, healthy instance becomes
available.
'''
import boto3, time, sys
from os.path import join, dirname, exists

version_paths = ['../openaddr/VERSION', 'VERSION']

def first_file(paths):
    for path in paths:
        if exists(join(dirname(__file__), path)):
            return join(dirname(__file__), path)

def main():
    '''
    '''
    asg_client = boto3.client('autoscaling', region_name='us-east-1')
    elb_client = boto3.client('elb', region_name='us-east-1')
    
    with open(first_file(version_paths)) as file:
        version = file.read().strip()
    
    print('Found version', version, file=sys.stderr)
    
    group_name = 'CI Webhooks {0}.x'.format(*version.split('.'))
    elb_name = 'default-openaddresses-io'
    
    print('Raising desired capacity of group', group_name, 'to', 2, file=sys.stderr)
    asg_client.set_desired_capacity(AutoScalingGroupName=group_name, DesiredCapacity=2)
    
    while True:
        response = asg_client.describe_auto_scaling_groups(AutoScalingGroupNames=[group_name])
        instances = response['AutoScalingGroups'][0]['Instances']
        count = len([i for i in instances if i['LifecycleState'] == 'InService'])
        
        if count == 2:
            print('- Yay,', count, 'in group', group_name, file=sys.stderr)
            break
        else:
            print('- Boo,', count, 'in group', group_name, file=sys.stderr)
            time.sleep(30)
    
    while True:
        response = elb_client.describe_instance_health(LoadBalancerName=elb_name)
        instances = response['InstanceStates']
        count = len([i for i in instances if i['State'] == 'InService'])
        
        if count == 2:
            print('- Yay,', count, 'in load balancer', elb_name, file=sys.stderr)
            break
        else:
            print('- Boo,', count, 'in load balancer', elb_name, file=sys.stderr)
            time.sleep(30)
    
    print('Lowering desired capacity of group', group_name, 'to', 1, file=sys.stderr)
    asg_client.set_desired_capacity(AutoScalingGroupName=group_name, DesiredCapacity=1)

if __name__ == '__main__':
    exit(main())
