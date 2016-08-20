import logging; _L = logging.getLogger('openaddr.ci.collect_supervise')

from os import environ
from time import time, sleep
from argparse import ArgumentParser

from ..util import request_task_instance
from . import setup_logger, log_function_errors
from .. import __version__

from boto import connect_autoscale, connect_ec2

parser = ArgumentParser(description='Run collection process on a remote EC2 instance.')

parser.add_argument('-o', '--owner', default='openaddresses',
                    help='Github repository owner. Defaults to "openaddresses".')

parser.add_argument('-r', '--repository', default='openaddresses',
                    help='Github repository name. Defaults to "openaddresses".')

parser.add_argument('-b', '--bucket', default='data.openaddresses.io',
                    help='S3 bucket name. Defaults to "data.openaddresses.io".')

parser.add_argument('-d', '--database-url', default=environ.get('DATABASE_URL', None),
                    help='Optional connection string for database. Defaults to value of DATABASE_URL environment variable.')

parser.add_argument('-a', '--access-key', default=environ.get('AWS_ACCESS_KEY_ID', None),
                    help='Optional AWS access key name. Defaults to value of AWS_ACCESS_KEY_ID environment variable.')

parser.add_argument('-s', '--secret-key', default=environ.get('AWS_SECRET_ACCESS_KEY', None),
                    help='Optional AWS secret key name. Defaults to value of AWS_SECRET_ACCESS_KEY environment variable.')

parser.add_argument('--sns-arn', default=environ.get('AWS_SNS_ARN', None),
                    help='Optional AWS Simple Notification Service (SNS) resource. Defaults to value of AWS_SNS_ARN environment variable.')

parser.add_argument('-v', '--verbose', help='Turn on verbose logging',
                    action='store_const', dest='loglevel',
                    const=logging.DEBUG, default=logging.INFO)

parser.add_argument('-q', '--quiet', help='Turn off most logging',
                    action='store_const', dest='loglevel',
                    const=logging.WARNING, default=logging.INFO)

@log_function_errors
def main():
    ''' 
    '''
    instance, deadline = False, time() + 12 * 3600

    args = parser.parse_args()
    setup_logger(args.sns_arn, log_level=args.loglevel)
    ec2 = connect_ec2(args.access_key, args.secret_key)
    autoscale = connect_autoscale(args.access_key, args.secret_key)

    try:
        ec2, autoscale = connect_ec2(), connect_autoscale()
        command = (
            'openaddr-collect-extracts', '--owner', args.owner,
            '--repository', args.repository, '--bucket', args.bucket,
            '--database-url', args.database_url, '--access-key', args.access_key,
            '--secret-key', args.secret_key, '--sns-arn', args.sns_arn
            )
        instance = request_task_instance(ec2, autoscale, 'm3.medium', 'openaddr', command)

        while True:
            instance.update()
            _L.debug('{:.0f} seconds to go, instance is {}...'.format(deadline - time(), instance.state))

            if instance.state == 'terminated':
                break

            if time() > deadline:
                _L.warning('Stopping instance {} at deadline'.format(instance))
                raise RuntimeError('Out of time')

            sleep(60)

        log_output = instance.get_console_output().output.decode('utf8')
        _L.info('Cloud-init log from EC2 instance:\n\n{}\n\n'.format(log_output))
        
    except:
        _L.error('Error in worker main()', exc_info=True)
        if instance:
            instance.terminate()
        return 1

    else:
        return 0

if __name__ == '__main__':
    exit(main())
