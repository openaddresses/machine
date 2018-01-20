#!/usr/bin/env python2
'''
Simple worker process to process OpenAddress sources on the task queue

Jobs get enqueued to a PQ task queue by some other system.
This program pops jobs and runs them one at a time, then
enqueues a new message on a separate PQ queue when the work is done.
'''
import logging; _L = logging.getLogger('openaddr.ci.worker')

from .. import S3

from argparse import ArgumentParser
import time, os, tempfile, shutil

from . import (
    db_connect, db_queue, db_queue, pop_task_from_taskqueue,
    DONE_QUEUE, TASK_QUEUE, DUE_QUEUE, setup_logger, HEARTBEAT_QUEUE,
    log_function_errors
    )

parser = ArgumentParser(description='Run some source files.')

parser.add_argument('-b', '--bucket', default=os.environ.get('AWS_S3_BUCKET', None),
                    help='S3 bucket name. Defaults to value of AWS_S3_BUCKET environment variable.')

parser.add_argument('-d', '--database-url', default=os.environ.get('DATABASE_URL', None),
                    help='Optional connection string for database. Defaults to value of DATABASE_URL environment variable.')

parser.add_argument('--sns-arn', default=os.environ.get('AWS_SNS_ARN', None),
                    help='Optional AWS Simple Notification Service (SNS) resource. Defaults to value of AWS_SNS_ARN environment variable.')

parser.add_argument('--mapbox-key', default=os.environ.get('MAPBOX_KEY', None),
                    help='Mapbox API Key. Defaults to value of MAPBOX_KEY environment variable. See: https://mapbox.com/')

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
    
    # Fetch and run jobs in a loop    
    while True:
        worker_dir = tempfile.mkdtemp(prefix='worker-')
    
        try:
            connection = db_connect(args.database_url)
            with connection as conn:
                task_Q = db_queue(conn, TASK_QUEUE)
                done_Q = db_queue(conn, DONE_QUEUE)
                due_Q = db_queue(conn, DUE_QUEUE)
                beat_Q = db_queue(conn, HEARTBEAT_QUEUE)
                pop_task_from_taskqueue(s3, task_Q, done_Q, due_Q, beat_Q,
                                        worker_dir, args.mapbox_key)
            connection.close()
        except:
            _L.error('Error in worker main()', exc_info=True)
            time.sleep(2)
        finally:
            shutil.rmtree(worker_dir)

if __name__ == '__main__':
    exit(main())
