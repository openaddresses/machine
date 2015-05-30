from sys import stderr
from time import sleep
from traceback import print_exc

from . import (
    db_connect, db_queue, pop_task_from_donequeue, DONE_QUEUE, load_config
    )

def main():
    '''
    '''
    config = load_config()
    
    while True:
        try:
            with db_connect(config['DATABASE_URL']) as conn:
                queue = db_queue(conn, DONE_QUEUE)
                pop_task_from_donequeue(queue, config['GITHUB_AUTH'])
        except:
            print >> stderr, '-' * 40
            print_exc(file=stderr)
            print >> stderr, '-' * 40
            sleep(5)

if __name__ == '__main__':
    exit(main())