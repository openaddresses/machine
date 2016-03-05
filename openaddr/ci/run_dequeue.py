import logging; _L = logging.getLogger('openaddr.ci.run_dequeue')

from os import environ
from time import sleep, time
from boto import connect_cloudwatch

from . import (
    db_connect, db_queue, TASK_QUEUE, DONE_QUEUE, DUE_QUEUE, load_config,
    pop_task_from_donequeue, pop_task_from_duequeue, setup_logger,
    HEARTBEAT_QUEUE, flush_heartbeat_queue, get_recent_workers
    )

def main():
    '''
    '''
    setup_logger(environ.get('AWS_SNS_ARN'))
    config = load_config()
    checkin_time = time()
    try:
        # Rely on boto AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY variables.
        cw = connect_cloudwatch()
    except:
        cw = False
    
    while True:
        try:
            with db_connect(config['DATABASE_URL']) as conn:
                task_Q = db_queue(conn, TASK_QUEUE)
                done_Q = db_queue(conn, DONE_QUEUE)
                due_Q = db_queue(conn, DUE_QUEUE)
                beat_Q = db_queue(conn, HEARTBEAT_QUEUE)

                pop_task_from_donequeue(done_Q, config['GITHUB_AUTH'])
                pop_task_from_duequeue(due_Q, config['GITHUB_AUTH'])
                flush_heartbeat_queue(beat_Q)
            
                if time() < checkin_time:
                    continue

                # Report basic information about current status.
                with beat_Q as db:
                    recent_workers = get_recent_workers(db)
                    workers_n = sum(map(len, recent_workers.values()))
                
                task_n, done_n, due_n = map(len, (task_Q, done_Q, due_Q))
                _L.info('{workers_n} active workers; queue lengths: {task_n} tasks, {done_n} done, {due_n} due'.format(**locals()))
                
                if cw:
                    cw.put_metric_data('openaddr.ci', 'tasks queue', task_n, unit='Count')
                    cw.put_metric_data('openaddr.ci', 'done queue', done_n, unit='Count')
                    cw.put_metric_data('openaddr.ci', 'due queue', due_n, unit='Count')
                    cw.put_metric_data('openaddr.ci', 'expected results', task_n + workers_n, unit='Count')
                    cw.put_metric_data('openaddr.ci', 'active workers', workers_n, unit='Count')

                checkin_time = time() + 30

        except KeyboardInterrupt:
            raise
        except:
            _L.error('Error in dequeue main()', exc_info=True)
            sleep(5)

if __name__ == '__main__':
    exit(main())