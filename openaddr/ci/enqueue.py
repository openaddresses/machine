import logging; _L = logging.getLogger('openaddr.ci.enqueue')

from os import environ
from time import sleep

from . import (
    db_connect, db_queue, TASK_QUEUE, load_config, setup_logger,
    enqueue_sources, find_batch_sources
    )

auth = environ['GITHUB_TOKEN'], 'x-oauth-basic'
start_url = 'https://api.github.com/repos/openaddresses/openaddresses'
start_url = 'https://api.github.com/repos/openaddresses/hooked-on-sources'


def main():
    ''' Single threaded worker to serve the job queue.
    '''
    setup_logger(environ.get('AWS_SNS_ARN'))
    config = load_config()

    try:
        sources = find_batch_sources(start_url, auth)

        with db_connect(config['DATABASE_URL']) as conn:
            task_Q = db_queue(conn, TASK_QUEUE)
            for _ in enqueue_sources(task_Q, sources, auth):
                print(_, len(task_Q))
                sleep(5)
    except:
        _L.error('Error in worker main()', exc_info=True)

if __name__ == '__main__':
    exit(main())
