import logging; _L = logging.getLogger('openaddr.ci.enqueue')

from os import environ
from time import time, sleep
from argparse import ArgumentParser

from . import (
    db_connect, db_queue, TASK_QUEUE, load_config, setup_logger,
    enqueue_sources, find_batch_sources
    )

parser = ArgumentParser(description='Run some source files.')

parser.add_argument('-o', '--owner', default='openaddresses',
                    help='Github repository owner. Defaults to "openaddresses".')

parser.add_argument('-r', '--repository', default='openaddresses',
                    help='Github repository name. Defaults to "openaddresses".')

parser.add_argument('-t', '--github-token', default=environ.get('GITHUB_TOKEN', None),
                    help='Optional token value for reading from Github. Defaults to value of GITHUB_TOKEN environment variable.')

parser.add_argument('-d', '--database-url', default=environ.get('DATABASE_URL', None),
                    help='Optional connection string for database. Defaults to value of DATABASE_URL environment variable.')

def main():
    ''' Single threaded worker to serve the job queue.
    '''
    args = parser.parse_args()
    setup_logger(environ.get('AWS_SNS_ARN'))
    github_auth = args.github_token, 'x-oauth-basic'

    try:
        sources = find_batch_sources(args.owner, args.repository, github_auth)

        with db_connect(args.database_url) as conn:
            task_Q = db_queue(conn, TASK_QUEUE)
            next_queue_report = time() + 60
            for _ in enqueue_sources(task_Q, sources):
                if time() >= next_queue_report:
                    next_queue_report, n = time() + 60, len(task_Q)
                    _L.debug('Task queue has {} item{}'.format(n, 's' if n != 1 else ''))
                sleep(5)
    except:
        _L.error('Error in worker main()', exc_info=True)

if __name__ == '__main__':
    exit(main())
