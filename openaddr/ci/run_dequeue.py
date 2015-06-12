import logging; _L = logging.getLogger('openaddr.ci.run_dequeue')

from time import sleep

from . import (
    db_connect, db_queue, DONE_QUEUE, DUE_QUEUE, load_config,
    pop_task_from_donequeue, pop_task_from_duequeue, setup_logger
    )

def main():
    '''
    '''
    setup_logger()
    config = load_config()
    
    while True:
        try:
            with db_connect(config['DATABASE_URL']) as conn:
                done_queue = db_queue(conn, DONE_QUEUE)
                pop_task_from_donequeue(done_queue, config['GITHUB_AUTH'])

                due_queue = db_queue(conn, DUE_QUEUE)
                pop_task_from_duequeue(due_queue, config['GITHUB_AUTH'])
        except KeyboardInterrupt:
            raise
        except:
            _L.error('Error in dequeue main()', exc_info=True)
            sleep(5)

if __name__ == '__main__':
    exit(main())