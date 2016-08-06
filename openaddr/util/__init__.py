from urllib.parse import urlparse, parse_qsl
from datetime import datetime, timedelta

from .. import __version__

def prepare_db_kwargs(dsn):
    '''
    '''
    p = urlparse(dsn)
    q = dict(parse_qsl(p.query))

    assert p.scheme == 'postgres'
    kwargs = dict(user=p.username, password=p.password, host=p.hostname, port=p.port)
    kwargs.update(dict(database=p.path.lstrip('/')))

    if 'sslmode' in q:
        kwargs.update(dict(sslmode=q['sslmode']))
    
    return kwargs

def set_autoscale_capacity(autoscale, cloudwatch, capacity):
    '''
    '''
    span, now = 60 * 60 * 3, datetime.now()
    start, end = now - timedelta(seconds=span), now
    args = 'tasks queue', 'openaddr.ci', 'Maximum'

    (measure, ) = cloudwatch.get_metric_statistics(span, start, end, *args)

    group_name = 'CI Workers {0}.x'.format(*__version__.split('.'))
    (group, ) = autoscale.get_all_groups([group_name])
    
    if group.desired_capacity >= capacity:
        return
    
    if measure['Maximum'] > .9:
        group.set_capacity(capacity)

def request_task_instance(ec2, autoscale):
    '''
    '''
    group_name = 'CI Workers {0}.x'.format(*__version__.split('.'))

    (group, ) = autoscale.get_all_groups([group_name])
    (config, ) = autoscale.get_all_launch_configurations(names=[group.launch_config_name])
    (image, ) = ec2.get_all_images(image_ids=[config.image_id])
    keypair = ec2.get_all_key_pairs()[0]

    run_kwargs = dict(instance_type='m3.medium', security_groups=['default'],
                      instance_initiated_shutdown_behavior='terminate',
                      key_name=keypair.name,
                      user_data='''#!/bin/bash
sleep 5
echo 'Walla walla walla'
shutdown -h now
''')

    reservation = image.run(**run_kwargs)
    (instance, ) = reservation.instances
    instance.add_tag('Name', 'Hell Yeah')
    
    return instance
