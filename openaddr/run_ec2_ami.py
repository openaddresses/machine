import logging; _L = logging.getLogger('openaddr.ci.run_ec2_ami')

from os import environ
from time import time, sleep
from argparse import ArgumentParser

from .util import request_task_instance
from .ci import setup_logger, log_function_errors
from . import __version__

from boto import connect_autoscale, connect_ec2

parser = ArgumentParser(description='Run a process on a remote EC2 instance.')

parser.add_argument('-a', '--access-key', help='Deprecated option provided for backwards compatibility.')
parser.add_argument('-s', '--secret-key', help='Deprecated option provided for backwards compatibility.')

parser.add_argument('-b', '--bucket', default=environ.get('AWS_S3_BUCKET', None),
                    help='S3 bucket name. Defaults to value of AWS_S3_BUCKET environment variable.')

parser.add_argument('--sns-arn', default=environ.get('AWS_SNS_ARN', None),
                    help='Optional AWS Simple Notification Service (SNS) resource. Defaults to value of AWS_SNS_ARN environment variable.')

parser.add_argument('--role', default='openaddr',
                    help='Machine chef role to execute. Defaults to "openaddr".')

parser.add_argument('--hours', default=12, type=float,
                    help='Number of hours to allow before giving up. Defaults to 12 hours.')

parser.add_argument('--instance-type', default='m3.medium',
                    help='EC2 instance type. Defaults to "m3.medium".')

parser.add_argument('--slack-url',
                    help='Slack POST URL.')

parser.add_argument('-v', '--verbose', help='Turn on verbose logging',
                    action='store_const', dest='loglevel',
                    const=logging.DEBUG, default=logging.INFO)

parser.add_argument('-q', '--quiet', help='Turn off most logging',
                    action='store_const', dest='loglevel',
                    const=logging.WARNING, default=logging.INFO)

parser.add_argument('command', nargs='*', help='Command with arguments to run on remote instance')

@log_function_errors
def main():
    ''' 
    '''
    args = parser.parse_args()
    instance, deadline, lifespan = False, time() + (args.hours + 1) * 3600, int(args.hours * 3600)
    setup_logger(args.access_key, args.secret_key, args.sns_arn, log_level=args.loglevel)

    try:
        ec2 = connect_ec2(None, None)
        autoscale = connect_autoscale(None, None)
        instance = request_task_instance(ec2, autoscale, args.instance_type,
                                         args.role, lifespan, args.command,
                                         args.bucket, args.slack_url)

        while True:
            instance.update()
            _L.debug('{:.0f} seconds to go, instance is {}...'.format(deadline - time(), instance.state))

            if instance.state == 'terminated':
                break

            if time() > deadline:
                log_instance_log(instance)
                _L.warning('Stopping instance {} at deadline'.format(instance))
                raise RuntimeError('Out of time')

            sleep(60)

        log_instance_log(instance)
        
    except:
        _L.error('Error in worker main()', exc_info=True)
        if instance:
            instance.terminate()
        return 1

    else:
        return 0

def log_instance_log(instance):
    '''
    '''
    log_output = instance.get_console_output().output.decode('utf8')
    _L.info('Cloud-init log from EC2 instance:\n\n{}\n\n'.format(log_output))

if __name__ == '__main__':
    exit(main())
