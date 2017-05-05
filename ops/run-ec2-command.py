#!/usr/bin/env python3
''' Invokes a fresh, single-use task runner on EC2 for OpenAddresses tasks.

This code lives in AWS Lambda, and is invoked from AWS Cloudwatch event rules
described in update-scheduled-tasks.py.
'''
import boto, shlex, os, time, pprint, sys
from boto.ec2 import blockdevicemapping
from boto.exception import EC2ResponseError
from os.path import join, dirname, exists
from datetime import datetime

version_paths = ['../openaddr/VERSION', 'VERSION']
userdata_paths = ['run-ec2-command-userdata.sh']

def first_file(paths):
    for path in paths:
        if exists(join(dirname(__file__), path)):
            return join(dirname(__file__), path)

def get_version():
    '''
    '''
    with open(first_file(version_paths)) as file:
        return next(file).strip()

def request_task_instance(ec2, autoscale, instance_type, lifespan, command, bucket, aws_sns_arn, patch_version, tempsize):
    '''
    '''
    major_version = patch_version.split('.')[0]
    group_name = 'CI Workers {0}.x'.format(major_version)

    (group, ) = autoscale.get_all_groups([group_name])
    (config, ) = autoscale.get_all_launch_configurations(names=[group.launch_config_name])
    (image, ) = ec2.get_all_images(image_ids=[config.image_id])
    keypair = [kp for kp in ec2.get_all_key_pairs() if kp.name.startswith('oa-')][0]

    yyyymmdd = datetime.utcnow().strftime('%Y-%m-%d-%H-%M')
    
    with open(first_file(userdata_paths)) as file:
        userdata_kwargs = dict(
            command = ' '.join(map(shlex.quote, command)),
            lifespan = shlex.quote(str(lifespan)),
            major_version = shlex.quote(major_version),
            patch_version = shlex.quote(patch_version),
            log_prefix = shlex.quote('logs/{}-{}'.format(yyyymmdd, command[0])),
            bucket = shlex.quote(bucket or 'data.openaddresses.io'),
            aws_sns_arn = '', aws_region = '',
            command_name = command[0]
            )
        
        if aws_sns_arn:
            try:
                _, _, _, aws_region, _ = aws_sns_arn.split(':', 4)
            except ValueError:
                pass
            else:
                if aws_sns_arn.startswith('arn:aws:sns:'):
                    userdata_kwargs.update(aws_sns_arn = shlex.quote(aws_sns_arn),
                                           aws_region = shlex.quote(aws_region))
    
        device_map = blockdevicemapping.BlockDeviceMapping()

        if tempsize:
            dev_sdb = blockdevicemapping.BlockDeviceType(delete_on_termination=True)
            dev_sdb.size = tempsize
            device_map['/dev/sdb'] = dev_sdb

        run_kwargs = dict(instance_type=instance_type, security_groups=['default'],
                          instance_initiated_shutdown_behavior='terminate',
                          # TODO: use current role from http://169.254.169.254/latest/meta-data/iam/info
                          instance_profile_name='machine-communication',
                          key_name=keypair.name, block_device_map=device_map)
        
        print('Configured with run kwargs:\n{}'.format(pprint.pformat(run_kwargs)), file=sys.stderr)
        
        run_kwargs.update(user_data=file.read().format(**userdata_kwargs))
        
        print('Configured with user data:\n{}'.format(run_kwargs['user_data']), file=sys.stderr)
        
    reservation = image.run(**run_kwargs)
    (instance, ) = reservation.instances
    
    try:
        instance.add_tag('Name', 'Scheduled {} {}'.format(yyyymmdd, command[0]))
    except EC2ResponseError:
        time.sleep(10)
        try:
            instance.add_tag('Name', 'Scheduled {} {}'.format(yyyymmdd, command[0]))
        except EC2ResponseError:
            time.sleep(10)
            instance.add_tag('Name', 'Scheduled {} {}'.format(yyyymmdd, command[0]))
    
    instance.add_tag('Command', command[0])
    instance.add_tag('Trigger', 'run-ec2-command')

    print('Started EC2 instance {} from AMI {}'.format(instance, image), file=sys.stderr)
    
    return instance

def main():
    ec2, autoscale = boto.connect_ec2(), boto.connect_autoscale()
    kwargs = dict(
        instance_type = 't2.nano',
        lifespan = 600,
        command = 'sleep 600'.split(),
        bucket = 'data.openaddresses.io',
        aws_sns_arn = 'arn:aws:sns:us-east-1:847904970422:CI-Events',
        patch_version = get_version(),
        tempsize = None,
        )
    
    return request_task_instance(ec2, autoscale, **kwargs)

def lambda_func(event, context):
    ''' Request a task instance inside AWS Lambda context.
    '''
    ec2, autoscale = boto.connect_ec2(), boto.connect_autoscale()
    kwargs = dict(
        instance_type = event.get('instance-type', 'm3.medium'),
        lifespan = int(event.get('hours', 12)) * 3600,
        command = event.get('command', ['sleep', '300']),
        bucket = event.get('bucket', os.environ.get('AWS_S3_BUCKET')),
        aws_sns_arn = event.get('sns-arn', os.environ.get('AWS_SNS_ARN')),
        patch_version = event.get('version', get_version()),
        tempsize = event.get('temp-size', None),
        )
    
    return str(request_task_instance(ec2, autoscale, **kwargs))

if __name__ == '__main__':
    exit(main())
