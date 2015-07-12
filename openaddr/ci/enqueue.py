import logging; _L = logging.getLogger('openaddr.ci.enqueue')

from os import environ

from . import db_connect, db_queue, load_config, setup_logger, enqueue_sources, TASK_QUEUE

auth = environ['GITHUB_TOKEN'], 'x-oauth-basic'
start_url = 'https://api.github.com/repos/openaddresses/openaddresses'
start_url = 'https://api.github.com/repos/openaddresses/hooked-on-sources'


def main():
    ''' Single threaded worker to serve the job queue.
    '''
    setup_logger(environ.get('AWS_SNS_ARN'))
    config = load_config()

    try:
        with db_connect(config['DATABASE_URL']) as conn:
            task_Q = db_queue(conn, TASK_QUEUE)
            enqueue_sources(task_Q, start_url, auth)
    except:
        _L.error('Error in worker main()', exc_info=True)
        time.sleep(5)

if __name__ == '__main__':
    exit(main())
