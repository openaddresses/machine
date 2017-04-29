#!/usr/bin/env python3
import logging; _L = logging.getLogger(__name__)

import boto, shlex
from boto.ec2 import blockdevicemapping
from os.path import join, dirname
from datetime import datetime

def get_version():
    '''
    '''
    path = join(dirname(__file__), 'VERSION')
    with open(path) as file:
        return next(file).strip()

def request_task_instance(ec2, autoscale, instance_type, lifespan, command, bucket, aws_sns_arn, tempsize=None):
    '''
    '''
    group_name = 'CI Workers {0}.x'.format(*get_version().split('.'))

    (group, ) = autoscale.get_all_groups([group_name])
    (config, ) = autoscale.get_all_launch_configurations(names=[group.launch_config_name])
    (image, ) = ec2.get_all_images(image_ids=[config.image_id])
    keypair = [kp for kp in ec2.get_all_key_pairs() if kp.name.startswith('oa-')][0]

    yyyymmdd = datetime.utcnow().strftime('%Y-%m-%d-%H-%M')
    
    with open(join(dirname(__file__), 'task-instance-userdata.sh')) as file:
        userdata_kwargs = dict(
            command = ' '.join(map(shlex.quote, command)),
            lifespan = shlex.quote(str(lifespan)),
            version = shlex.quote(get_version()),
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
                          user_data=file.read().format(**userdata_kwargs),
                          # TODO: use current role from http://169.254.169.254/latest/meta-data/iam/info
                          instance_profile_name='machine-communication',
                          key_name=keypair.name, block_device_map=device_map)
        
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
    
    _L.info('Started EC2 instance {} from AMI {}'.format(instance, image))
    
    return instance

def main():
    kwargs = dict(
        ec2 = boto.connect_ec2(),
        autoscale = boto.connect_autoscale(),
        instance_type = 't2.nano',
        lifespan = 600,
        command = 'sleep 600'.split(),
        bucket = 'data.openaddresses.io',
        aws_sns_arn = 'arn:aws:sns:us-east-1:847904970422:CI-Events',
        )
    
    return request_task_instance(**kwargs)

def lambda_func(event, context):
    return main()

if __name__ == '__main__':
    exit(main())
