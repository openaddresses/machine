import logging; _L = logging.getLogger('openaddr.ci.enqueue')

from os import environ
from itertools import count
from time import time, sleep
from argparse import ArgumentParser

from . import (
    db_connect, db_queue, TASK_QUEUE, load_config, setup_logger,
    enqueue_sources, find_batch_sources, get_batch_run_times
    )

from .objects import add_set
from ..util import set_autoscale_capacity
from . import render_set_maps, log_function_errors
from .. import S3

from boto import connect_autoscale, connect_cloudwatch

parser = ArgumentParser(description='Run some source files.')

parser.add_argument('-o', '--owner', default='openaddresses',
                    help='Github repository owner. Defaults to "openaddresses".')

parser.add_argument('-r', '--repository', default='openaddresses',
                    help='Github repository name. Defaults to "openaddresses".')

parser.add_argument('-t', '--github-token', default=environ.get('GITHUB_TOKEN', None),
                    help='Optional token value for reading from Github. Defaults to value of GITHUB_TOKEN environment variable.')

parser.add_argument('-d', '--database-url', default=environ.get('DATABASE_URL', None),
                    help='Optional connection string for database. Defaults to value of DATABASE_URL environment variable.')

parser.add_argument('-b', '--bucket', default=environ.get('AWS_S3_BUCKET', None),
                    help='S3 bucket name. Defaults to value of AWS_S3_BUCKET environment variable.')

parser.add_argument('--sns-arn', default=environ.get('AWS_SNS_ARN', None),
                    help='Optional AWS Simple Notification Service (SNS) resource. Defaults to value of AWS_SNS_ARN environment variable.')

parser.add_argument('--cloudwatch-ns', default=environ.get('AWS_CLOUDWATCH_NS', None),
                    help='Optional AWS CloudWatch namespace. Defaults to value of AWS_CLOUDWATCH_NS environment variable.')

parser.add_argument('-v', '--verbose', help='Turn on verbose logging',
                    action='store_const', dest='loglevel',
                    const=logging.DEBUG, default=logging.INFO)

parser.add_argument('-q', '--quiet', help='Turn off most logging',
                    action='store_const', dest='loglevel',
                    const=logging.WARNING, default=logging.INFO)

@log_function_errors
def main():
    ''' Single threaded worker to serve the job queue.
    '''
    args = parser.parse_args()
    setup_logger(args.sns_arn, None, log_level=args.loglevel)
    s3 = S3(None, None, args.bucket)
    autoscale = connect_autoscale(None, None)
    cloudwatch = connect_cloudwatch(None, None)
    github_auth = args.github_token, 'x-oauth-basic'

    next_queue_interval, next_autoscale_interval = 60, 8 * 3600

    try:
        with db_connect(args.database_url) as conn:
            task_Q = db_queue(conn, TASK_QUEUE)
            next_queue_report = time() + next_queue_interval
            next_autoscale_grow = time() + next_autoscale_interval
            minimum_capacity = count(1)
        
            with task_Q as db:
                run_times = get_batch_run_times(db, args.owner, args.repository)

            sources = find_batch_sources(args.owner, args.repository, github_auth, run_times)
            
            with task_Q as db:
                new_set = add_set(db, args.owner, args.repository)

            for expected_count in enqueue_sources(task_Q, new_set, sources):
                if time() >= next_queue_report:
                    next_queue_report, n = time() + next_queue_interval, len(task_Q)
                    _L.debug('Task queue has {} item{}, {} sources expected'.format(n, 's' if n != 1 else '', expected_count))
                try:
                    if time() >= next_autoscale_grow:
                        next_autoscale_grow = time() + next_autoscale_interval
                        set_autoscale_capacity(autoscale, cloudwatch, args.cloudwatch_ns, next(minimum_capacity))
                except Exception as e:
                    _L.error('Problem during autoscale', exc_info=True)
                if expected_count:
                    sleep(2)
        
        with task_Q as db:
            _L.debug('Rendering that shit')
            render_set_maps(s3, db, new_set)
        
    except:
        _L.error('Error in worker main()', exc_info=True)
        return 1

    else:
        return 0

if __name__ == '__main__':
    exit(main())
