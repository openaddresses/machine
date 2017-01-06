#!/usr/bin/env python3
import subprocess, json, itertools

data = subprocess.check_output('aws ec2 describe-instances', shell=True)

instances = itertools.chain(*[
    reservation['Instances'] for reservation
    in json.loads(data.decode('utf8'))['Reservations']
    ])

for instance in sorted(instances, key=lambda inst: inst['State']['Code']):
    tags = {tag['Key']: tag['Value'] for tag in instance.get('Tags', {})}
    
    print('{:11} {:20} {} - {}'.format(
        instance['State']['Name'], instance['InstanceId'],
        tags.get('Name'), instance['PublicDnsName'],
        ))
