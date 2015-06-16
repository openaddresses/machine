#!/usr/bin/env python2
'''
Simple worker process to process OpenAddress sources on the task queue

Jobs get enqueued to a PQ task queue by some other system.
This program pops jobs and runs them one at a time, then
enqueues a new message on a separate PQ queue when the work is done.
'''
import logging; _L = logging.getLogger('openaddr.ci.worker')

from .. import compat
from ..jobs import JOB_TIMEOUT

import time, os, psycopg2, socket, json, tempfile
from urllib.parse import urlparse, urljoin

from . import (
    db_connect, db_queue, db_queue, pop_task_from_taskqueue,
    MAGIC_OK_MESSAGE, DONE_QUEUE, TASK_QUEUE, DUE_QUEUE, setup_logger
    )

def do_work(job_contents, output_dir):
    "Do the actual work of running a source file in job_contents"

    # Make a directory to run the whole job
    out_dir = tempfile.mkdtemp(prefix='work-', dir=output_dir)
    os.chmod(out_dir, 0o755)

    # Write the user input to a file
    out_fn = os.path.join(out_dir, 'user_input.txt')
    with open(out_fn, 'wb') as out_fp:
        out_fp.write(job_contents.encode('utf8'))

    # Make a directory in which to run openaddr
    oa_dir = os.path.join(out_dir, 'out')
    os.mkdir(oa_dir)

    # Invoke the job to do
    cmd = 'openaddr-process-one', '-l', os.path.join(out_dir, 'logfile.txt'), out_fn, oa_dir
    try:
        timeout_seconds = JOB_TIMEOUT.seconds + JOB_TIMEOUT.days * 86400
        result_stdout = compat.check_output(cmd, timeout=timeout_seconds)
        if hasattr(result_stdout, 'decode'):
            # "The actual encoding of the output data may depend on the command
            # being invoked" - https://docs.python.org/3/library/subprocess.html
            result_stdout = result_stdout.decode('utf8', 'replace')

    except compat.CalledProcessError as e:
        # Something went wrong; throw back an error result.
        return dict(result_code=e.returncode, result_stdout=e.output,
                    message='Something went wrong in {0}'.format(*cmd))

    result = dict(result_code=0, result_stdout=result_stdout,
                  message=MAGIC_OK_MESSAGE)

    # openaddr-process-one prints a path to index.json
    state_fullpath = result_stdout.strip()
    output_base = 'http://{host}/oa-runone/'.format(host=socket.getfqdn())
    state_path = os.path.relpath(result_stdout.strip(), output_dir)
    result['output_url'] = urljoin(output_base, state_path)

    with open(state_fullpath) as file:
        index = dict(zip(*json.load(file)))
        
        for key in ('processed', 'sample', 'cache'):
            if not index[key]:
                result.update(result_code=-1, message='Failed to produce {} data'.format(key))
        
        # Expand filename keys to complete URLs
        keys = 'cache', 'sample', 'output', 'processed'
        urls = [urljoin(result['output_url'], index[k]) for k in keys]
        
        result['output'] = index
        result['output'].update(dict(zip(keys, urls)))

    return result

def main():
    ''' Single threaded worker to serve the job queue.
    '''
    setup_logger(os.environ.get('AWS_SNS_ARN'))

    # File path and URL path for result directory. Should be S3.
    web_docroot = os.environ.get('WEB_DOCROOT', '/var/www/html')
    web_output_dir = os.path.join(web_docroot, 'oa-runone')

    # Fetch and run jobs in a loop    
    while True:
        try:
            with db_connect(os.environ['DATABASE_URL']) as conn:
                task_Q = db_queue(conn, TASK_QUEUE)
                done_Q = db_queue(conn, DONE_QUEUE)
                due_Q = db_queue(conn, DUE_QUEUE)
                pop_task_from_taskqueue(task_Q, done_Q, due_Q, web_output_dir)
        except:
            _L.error('Error in worker main()', exc_info=True)
            time.sleep(5)

if __name__ == '__main__':
    exit(main())
