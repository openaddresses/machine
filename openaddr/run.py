from __future__ import absolute_import, division, print_function
import logging; _L = logging.getLogger('openaddr.run')

from os import environ
from os.path import join, dirname
from argparse import ArgumentParser
from operator import attrgetter
from itertools import groupby
from time import time, sleep
from subprocess import check_call
from tempfile import mkdtemp
from shutil import rmtree
import socket

from . import jobs, __version__
from boto.ec2 import EC2Connection
from boto.ec2.blockdevicemapping import BlockDeviceMapping, BlockDeviceType
from paramiko.client import SSHClient, AutoAddPolicy

OVERDUE_SPOT_REQUEST = 'Out of time opening a spot instance request'
OVERDUE_SPOT_INSTANCE = 'Out of time receiving a spot instance'
OVERDUE_INSTANCE_DNS = 'Out of time getting an instance DNS name'
OVERDUE_UPLOAD_TARBALL = 'Out of time connecting to instance with SSH'
OVERDUE_PROCESS_ALL = 'Out of time processing all sources'

CHEAPSKATE='bid cheaply'
BIGSPENDER='bid dearly'

def get_bid_amount(ec2, instance_type, strategy=CHEAPSKATE):
    ''' Get a bid estimate for a given instance type.
    
        Returns median price + $0.01 for a selected AWS availability zone.
        Zone decided based on strategy, either CHEAPSKATE or BIGSPENDER.
    '''
    history = ec2.get_spot_price_history(instance_type=instance_type)

    get_az = attrgetter('availability_zone')
    median = 1.00
    
    for (zone, zone_history) in groupby(sorted(history, key=get_az), get_az):
        zone_prices = [h.price for h in zone_history]
        zone_median = sorted(zone_prices)[len(zone_prices)//2]

        _L.debug('Median ${:.4f}/hour in {} zone'.format(zone_median, zone))
        
        if strategy is CHEAPSKATE:
            median = min(median, zone_median)
        elif strategy is BIGSPENDER:
            median = max(median, zone_median)
        else:
            raise ValueError('Unknown bid strategy, "{}"'.format(strategy))
    
    return median + 0.01

def prepare_tarball(tempdir, repository, branch):
    ''' Return path to a new tarball from the repository at branch.
    '''
    clonedir = join(tempdir, 'repo')
    tarpath = join(tempdir, 'archive.tar')
    
    check_call(('git', 'clone', '-q', '-b', branch, '--bare', repository, clonedir))
    check_call(('git', '--git-dir', clonedir, 'archive', branch, '-o', tarpath))
    check_call(('gzip', tarpath))

    rmtree(clonedir)
    return tarpath + '.gz'

def connect_ssh(dns_name, identity_file):
    '''
    '''
    client = SSHClient()
    client.set_missing_host_key_policy(AutoAddPolicy())
    client.connect(dns_name, username='ubuntu', key_filename=identity_file)
    
    return client

parser = ArgumentParser(description='Run some source files.')

parser.add_argument('bucketname',
                    help='Required S3 bucket name.')

parser.add_argument('-r', '--repository', default='https://github.com/openaddresses/machine',
                    help='Optional git repository to clone. Defaults to "https://github.com/openaddresses/machine".')

parser.add_argument('-b', '--branch', default=__version__,
                    help='Optional git branch to clone. Defaults to OpenAddresses-Machine version, "{}".'.format(__version__))

parser.add_argument('-a', '--access-key', default=environ.get('AWS_ACCESS_KEY_ID', None),
                    help='Optional AWS access key name for writing to S3. Defaults to value of AWS_ACCESS_KEY_ID environment variable.')

parser.add_argument('-s', '--secret-key', default=environ.get('AWS_SECRET_ACCESS_KEY', None),
                    help='Optional AWS secret key name for writing to S3. Defaults to value of AWS_SECRET_ACCESS_KEY environment variable.')

parser.add_argument('-i', '--identity-file',
                    help='Optional SSH identity file for connecting to running EC2 instance. Should match EC2_SSH_KEYPAIR.')

parser.add_argument('--ec2-access-key',
                    help='Optional AWS access key name for setting up EC2; distinct from access key for populating S3 bucket. Defaults to value of EC2_ACCESS_KEY_ID environment variable or S3 access key.')

parser.add_argument('--ec2-secret-key',
                    help='Optional AWS secret key name for setting up EC2; distinct from secret key for populating S3 bucket. Defaults to value of EC2_SECRET_ACCESS_KEY environment variable or S3 secret key.')

parser.add_argument('--ec2-instance-type', '--instance-type', default='m3.xlarge',
                    help='EC2 instance type, defaults to m3.xlarge.')

parser.add_argument('--ec2-ssh-keypair', '--ssh-keypair', default='oa-keypair',
                    help='EC2 SSH key pair name, defaults to "oa-keypair".')

parser.add_argument('--ec2-security-group', '--security-group', default='default',
                    help='EC2 security group name, defaults to "default".')

parser.add_argument('--ec2-machine-image', '--machine-image', default='ami-4ae27e22',
                    help='AMI identifier, defaults to Alestic Ubuntu 14.04 (ami-4ae27e22).')

parser.add_argument('--cheapskate', dest='bid_strategy',
                    const=CHEAPSKATE, default=CHEAPSKATE, action='store_const',
                    help='Bid a low EC2 spot price, good for times of price stability.')

parser.add_argument('--bigspender', dest='bid_strategy',
                    const=BIGSPENDER, default=CHEAPSKATE, action='store_const',
                    help='Bid a high EC2 spot price, better for times of price volatility.')

def main():
    args = parser.parse_args()
    jobs.setup_logger(None)
    run_ec2(args)

def run_ec2(args):
    tempdir = mkdtemp(prefix='oa-')
    tarball = prepare_tarball(tempdir, args.repository, args.branch)
    
    _L.info('Created repository archive at {}'.format(tarball))
    
    #
    # Prepare init script for new EC2 instance to run.
    #
    with open(join(dirname(__file__), 'templates', 'user-data.sh')) as file:
        user_data = file.read().format(**args.__dict__)
    
    _L.info('Prepared {} bytes of instance user data for tag {}'.format(len(user_data), args.branch))

    #
    # Figure out how much we're willing to bid on a spot instance.
    #
    ec2_access_key = args.ec2_access_key or environ.get('EC2_ACCESS_KEY_ID', args.access_key)
    ec2_secret_key = args.ec2_secret_key or environ.get('EC2_SECRET_ACCESS_KEY', args.secret_key)
    ec2 = EC2Connection(ec2_access_key, ec2_secret_key)
    
    bid = get_bid_amount(ec2, args.ec2_instance_type, args.bid_strategy)
    _L.info('Bidding ${:.4f}/hour for {} instance'.format(bid, args.ec2_instance_type))
    
    #
    # Request a spot instance with 200GB storage.
    #
    device_sda1 = BlockDeviceType(size=200, delete_on_termination=True)
    device_map = BlockDeviceMapping(); device_map['/dev/sda1'] = device_sda1
    
    spot_args = dict(instance_type=args.ec2_instance_type, user_data=user_data,
                     key_name=args.ec2_ssh_keypair, block_device_map=device_map,
                     security_groups=[args.ec2_security_group])

    spot_req = ec2.request_spot_instances(bid, args.ec2_machine_image, **spot_args)[0]

    _L.info('https://console.aws.amazon.com/ec2/v2/home?region=us-east-1#SpotInstances:search={}'.format(spot_req.id))
    
    #
    # Wait while EC2 does its thing, unless the user interrupts.
    #
    try:
        instance = wait_for_setup(spot_req, time() + 15 * 60)
        upload_tarball(tarball, instance.public_dns_name, args.identity_file, time() + 3 * 60)
        wait_for_process(instance, time() + 12 * 60 * 60)
    
    except RuntimeError as e:
        _L.warning(e.message)

        if e.message is OVERDUE_PROCESS_ALL:
            # Instance was set up, but ran out of time. Get its log.
            logfile = join(tempdir, 'cloud-init-output.log')
            client = connect_ssh(instance.public_dns_name, args.identity_file)
            client.open_sftp().get('/var/log/cloud-init-output.log', logfile)
            
            with open(logfile) as file:
                _L.info('/var/log/cloud-init-output.log contents:\n\n{}\n'.format(file.read()))

    finally:
        spot_req = ec2.get_all_spot_instance_requests(spot_req.id)[0]
        
        if spot_req.instance_id:
            print('Shutting down instance {} early'.format(spot_req.instance_id))
            ec2.terminate_instances(spot_req.instance_id)
        
        spot_req.cancel()
        rmtree(tempdir)

def wait_for_setup(spot_req, due):
    ''' Wait for EC2 to finish its work.
    '''
    ec2 = spot_req.connection

    _L.info('Settling in for a short wait, up to {:.0f} minutes.'.format((due - time()) / 60))
    
    while True:
        sleep(15)
        spot_req = ec2.get_all_spot_instance_requests(spot_req.id)[0]
        if time() > due:
            raise RuntimeError(OVERDUE_SPOT_REQUEST)
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
            raise RuntimeError(OVERDUE_SPOT_INSTANCE)
        elif spot_req.instance_id:
            break
        else:
            _L.debug('Waiting for instance ID')
    
    while True:
        sleep(5)
        instance = ec2.get_only_instances(spot_req.instance_id)[0]
        if time() > due:
            raise RuntimeError(OVERDUE_INSTANCE_DNS)
        elif instance.public_dns_name:
            break
        else:
            _L.debug('Waiting for instance DNS name')

    _L.info('Found instance {} at {}'.format(instance.id, instance.public_dns_name))
    
    return instance

def upload_tarball(tarball, dns_name, identity_file, due):
    '''
    ''' 
    _L.info('Connecting to instance {} with {}'.format(dns_name, identity_file))

    while True:
        sleep(10)
        try:
            client = connect_ssh(dns_name, identity_file)
        except socket.error:
            if time() > due:
                raise RuntimeError(OVERDUE_UPLOAD_TARBALL)
            else:
                _L.debug('Waiting to connect to {}'.format(dns_name))
        else:
            break
    
    _L.info('Uploading {} to instance'.format(tarball))
    
    def progress(sent, total):
        return
        _L.debug('Sent {} of {} bytes'.format(sent, total))

    client.open_sftp().put(tarball, '/tmp/uploading.tar.gz', progress)
    client.exec_command('mv /tmp/uploading.tar.gz /tmp/machine.tar.gz')

def wait_for_process(instance, due):
    ''' Wait for EC2 to finish its work.
    '''
    ec2 = instance.connection

    _L.info('Settling in for the long wait, up to {:.0f} hours.'.format((due - time()) / 3600))
    
    while True:
        sleep(60)
        instance = ec2.get_only_instances(instance.id)[0]
        if time() > due:
            raise RuntimeError(OVERDUE_PROCESS_ALL)
        elif instance.state == 'terminated':
            _L.debug('Instance {} has been terminated'.format(instance.id))
            break
        else:
            _L.debug('Waiting for instance {} to do its work'.format(instance.id))

    _L.info('Job complete')

if __name__ == '__main__':
    exit(main())
