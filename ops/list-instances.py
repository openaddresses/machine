#!/usr/bin/env python3
import boto3, itertools

ec2_client = boto3.client('ec2')

instances = itertools.chain(*[
    reservation['Instances'] for reservation
    in ec2_client.describe_instances()['Reservations']
    ])

for instance in sorted(instances, key=lambda inst: inst['State']['Code']):
    tags = {tag['Key']: tag['Value'] for tag in instance.get('Tags', {})}
    
    print('{:11} {:20} {} - {}'.format(
        instance['State']['Name'], instance['InstanceId'],
        tags.get('Name'), instance['PublicDnsName'],
        ))
