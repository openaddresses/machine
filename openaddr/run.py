import logging; _L = logging.getLogger('openaddr.run')

from os import environ
from os.path import join, dirname
from argparse import ArgumentParser
from operator import attrgetter
from itertools import groupby
from time import time, sleep

from . import jobs
from boto.ec2 import EC2Connection
from boto.ec2.blockdevicemapping import BlockDeviceMapping, BlockDeviceType

def get_bid_amount(ec2, instance_type):
    ''' Get a bid estimate for a given instance type.
    
        Returns median price + $0.01 for the cheapest AWS availability zone.
    '''
    history = ec2.get_spot_price_history(instance_type=instance_type)

    get_az = attrgetter('availability_zone')
    median = 1.00
    
    for (zone, zone_history) in groupby(sorted(history, key=get_az), get_az):
        zone_prices = [h.price for h in zone_history]
        zone_median = sorted(zone_prices)[len(zone_prices)/2]

        _L.debug('Median ${:.4f}/hour in {} zone'.format(zone_median, zone))
        median = min(median, zone_median)
    
    return median + 0.01

parser = ArgumentParser(description='Run some source files.')

parser.add_argument('bucketname',
                    help='Required S3 bucket name.')

parser.add_argument('-a', '--access-key', default=environ.get('AWS_ACCESS_KEY_ID', None),
                    help='Optional AWS access key name for writing to S3. Defaults to value of AWS_ACCESS_KEY_ID environment variable.')

parser.add_argument('-s', '--secret-key', default=environ.get('AWS_SECRET_ACCESS_KEY', None),
                    help='Optional AWS secret key name for writing to S3. Defaults to value of AWS_SECRET_ACCESS_KEY environment variable.')

parser.add_argument('--ec2-access-key',
                    help='Optional AWS access key name for setting up EC2; distinct from access key for populating S3 bucket. Defaults to value of EC2_ACCESS_KEY_ID environment variable or S3 access key.')

parser.add_argument('--ec2-secret-key',
                    help='Optional AWS secret key name for setting up EC2; distinct from secret key for populating S3 bucket. Defaults to value of EC2_SECRET_ACCESS_KEY environment variable or S3 secret key.')

parser.add_argument('--instance-type', default='m3.xlarge',
                    help='EC2 instance type, defaults to m3.xlarge.')

parser.add_argument('--ssh-keypair', default='oa-keypair',
                    help='SSH key pair name, defaults to "oa-keypair".')

parser.add_argument('--security-group', default='default',
                    help='EC2 security group name, defaults to "default".')

parser.add_argument('--machine-image', default='ami-4ae27e22',
                    help='AMI identifier, defaults to Alestic Ubuntu 14.04 (ami-4ae27e22).')

def main():
    args = parser.parse_args()
    jobs.setup_logger(None)
    
    #
    # Prepare init script for new EC2 instance to run.
    #
    with open(join(dirname(__file__), 'templates', 'user-data.sh')) as file:
        user_data = file.read().format(**args.__dict__)
    
    _L.info('Prepared {} bytes of instance user data'.format(len(user_data)))

    #
    # Figure out how much we're willing to bid on a spot instance.
    #
    ec2_access_key = args.ec2_access_key or environ.get('EC2_ACCESS_KEY_ID', args.access_key)
    ec2_secret_key = args.ec2_secret_key or environ.get('EC2_SECRET_ACCESS_KEY', args.secret_key)
    ec2 = EC2Connection(ec2_access_key, ec2_secret_key)
    
    bid = get_bid_amount(ec2, args.instance_type)
    _L.info('Bidding ${:.4f}/hour for {} instance'.format(bid, args.instance_type))
    
    #
    # Request a spot instance with 200GB storage.
    #
    device_sda1 = BlockDeviceType(size=200, delete_on_termination=True)
    device_map = BlockDeviceMapping(); device_map['/dev/sda1'] = device_sda1
    
    spot_args = dict(instance_type=args.instance_type, user_data=user_data,
                     key_name=args.ssh_keypair, block_device_map=device_map,
                     security_groups=[args.security_group])

    spot_req = ec2.request_spot_instances(bid, args.machine_image, **spot_args)[0]

    _L.info('https://console.aws.amazon.com/ec2/v2/home?region=us-east-1#SpotInstances:search={}'.format(spot_req.id))
    
    #
    # Wait while EC2 does its thing, unless the user interrupts.
    #
    try:
        wait_it_out(spot_req, time() + 12 * 60 * 60)

    finally:
        spot_req = ec2.get_all_spot_instance_requests(spot_req.id)[0]
        
        if spot_req.instance_id:
            print 'Shutting down instance {} early'.format(spot_req.instance_id)
            ec2.terminate_instances(spot_req.instance_id)
        
        spot_req.cancel()

def wait_it_out(spot_req, due):
    ''' Wait for EC2 to finish its work.
    '''
    ec2 = spot_req.connection

    _L.info('Settling in for the long wait, up to {:.0f} hours.'.format((due - time()) / 3600))
    
    while True:
        sleep(15)
        spot_req = ec2.get_all_spot_instance_requests(spot_req.id)[0]
        if time() > due:
            raise RuntimeError('Out of time')
        elif spot_req.state == 'open':
            _L.debug('Spot request {} is open'.format(spot_req.id))
        else:
            break
    
    if spot_req.state != 'active':
        raise Exception('Unexpected spot request state "{}"'.format(spot_req.state))
    
    while True:
        sleep(5)
        spot_req = ec2.get_all_spot_instance_requests(spot_req.id)[0]
        if time() > due:
            raise RuntimeError('Out of time')
        elif spot_req.instance_id:
            break
        else:
            _L.debug('Waiting for instance ID')
    
    while True:
        sleep(5)
        instance = ec2.get_only_instances(spot_req.instance_id)[0]
        if time() > due:
            raise RuntimeError('Out of time')
        elif instance.public_dns_name:
            break
        else:
            _L.debug('Waiting for instance DNS name')

    _L.info('Found instance {} at {}'.format(instance.id, instance.public_dns_name))

    while True:
        sleep(60)
        instance = ec2.get_only_instances(instance.id)[0]
        if time() > due:
            raise RuntimeError('Out of time')
        elif instance.state == 'terminated':
            _L.debug('Instance {} has been terminated'.format(instance.id))
            break
        else:
            _L.debug('Waiting for instance {} to do its work'.format(instance.id))

    _L.info('Job complete')

if __name__ == '__main__':
    exit(main())
