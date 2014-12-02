from os import environ
from logging import getLogger
from argparse import ArgumentParser
from time import sleep

from . import jobs
from boto.ec2 import EC2Connection

parser = ArgumentParser(description='Run some source files.')

parser.add_argument('bucketname',
                    help='Required S3 bucket name.')

parser.add_argument('-a', '--access-key', default=environ.get('AWS_ACCESS_KEY_ID', None),
                    help='Optional AWS access key name. Defaults to value of AWS_ACCESS_KEY_ID environment variable.')

parser.add_argument('-s', '--secret-key', default=environ.get('AWS_SECRET_ACCESS_KEY', None),
                    help='Optional AWS secret key name. Defaults to value of AWS_SECRET_ACCESS_KEY environment variable.')

parser.add_argument('--ec2-access-key',
                    help='Optional AWS access key name for setting up EC2; distinct from access key for populating S3 bucket. Defaults to value of S3 access key.')

parser.add_argument('--ec2-secret-key',
                    help='Optional AWS secret key name for setting up EC2; distinct from secret key for populating S3 bucket. Defaults to value of S3 secret key.')

parser.add_argument('--instance-type', default='m3.xlarge',
                    help='EC2 instance type, defaults to m3.xlarge.')

parser.add_argument('--machine-image', default='ami-4ae27e22',
                    help='AMI identifier, defaults to Alestic Ubuntu 14.04 (ami-4ae27e22).')

def main():
    args = parser.parse_args()
    jobs.setup_logger(None)
    
    user_data = '''#!/bin/sh -ex
apt-get update -y
apt-get install -y git apache2

echo 'cloning' > /var/www/html/state.txt
git clone https://github.com/openaddresses/machine /tmp/machine

echo 'installing' > /var/www/html/state.txt
apt-get install -y build-essential ruby ruby-dev
gem install ohai -v 7.4.0 --no-rdoc --no-ri
gem install chef --no-rdoc --no-ri
/tmp/machine/chef/run.sh

echo 'processing' > /var/www/html/state.txt
openaddr-process -a {access_key} -s {secret_key} -l log.txt {bucketname}

echo 'killing' > /var/www/html/state.txt
python <<KILL

from urllib import urlopen
from boto.ec2 import EC2Connection

instance = urlopen('http://169.254.169.254/latest/meta-data/instance-id').read().strip()
EC2Connection('{ec2_access_key}', '{ec2_secret_key}').terminate_instances(instance)

KILL
'''.format(**args.__dict__)
    
    getLogger('openaddr').info('Prepared {} bytes of instance user data\n\n{}'.format(len(user_data), user_data))

    ec2 = EC2Connection(args.ec2_access_key, args.ec2_secret_key)
    history = ec2.get_spot_price_history(instance_type=args.instance_type)
    median = sorted([h.price for h in history])[len(history)/2]
    bid = median + .01

    getLogger('openaddr').info('Bidding ${:.4f}/hour'.format(bid))
    
    spot_args = dict(instance_type=args.instance_type, user_data=user_data,
                     key_name='cfa-keypair-2013', security_groups=['default'])

    spot_req = ec2.request_spot_instances(bid, args.machine_image, **spot_args)[0]

    url = 'https://console.aws.amazon.com/ec2/v2/home?region=us-east-1#SpotInstances:search={}'.format(spot_req.id)

    getLogger('openaddr').info(url)
    
    while spot_req.state == 'open':
        getLogger('openaddr').debug('Spot request is {}'.format(spot_req.status))
        sleep(15)
        spot_req = ec2.get_all_spot_instance_requests(spot_req.id)[0]
    
    if spot_req.state != 'active':
        raise Exception('FUCK')
    
    while not spot_req.instance_id:
        getLogger('openaddr').debug('Waiting for instance ID')
        sleep(15)
        spot_req = ec2.get_all_spot_instance_requests(spot_req.id)[0]
    
    getLogger('openaddr').info('Looking up instance {}'.format(spot_req.instance_id))
    instance = ec2.get_only_instances(spot_req.instance_id)[0]
    
    getLogger('openaddr').info('Found instance {}'.format(instance.public_dns_name))
    sleep(10)
    instance.terminate()

if __name__ == '__main__':
    exit(main())
