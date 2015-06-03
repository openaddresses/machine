#!/usr/bin/env python2
'''
Simple worker process to process OpenAddress sources on the task queue

Jobs get enqueued to a PQ task queue by some other system.
This program pops jobs and runs them one at a time, then
enqueues a new message on a separate PQ queue when the work is done.
'''
import logging; _L = logging.getLogger('openaddr.ci.worker')

from ..compat import standard_library

import time, os, subprocess, psycopg2, socket, json
from urllib.parse import urlparse, urljoin

from . import db_connect, db_queue, db_queue, MAGIC_OK_MESSAGE, DONE_QUEUE

# File path and URL path for result directory. Should be S3.
_web_output_dir = '/var/www/html/oa-runone'

def do_work(job_id, job_contents, output_dir):
    "Do the actual work of running a source file in job_contents"

    # Make a directory to run the whole job
    assert '/' not in job_id
    out_dir = '%s/%s' % (output_dir, job_id)
    os.mkdir(out_dir)

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
        result_stdout = subprocess.check_output(cmd)

    except subprocess.CalledProcessError as e:
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
        
        # Expand filename keys to complete URLs
        keys = 'cache', 'sample', 'output', 'processed'
        urls = [urljoin(result['output_url'], index[k]) for k in keys]
        
        result['output'] = index
        result['output'].update(dict(zip(keys, urls)))

    return result

def run(task_data, output_dir):
    ''' Run a task posted to the queue. Handles the JSON for task and result objects.
    '''
    _L.info("Got job {}".format(task_data))

    # Unpack the task object
    content = task_data['content']
    url = task_data['url']
    job_id = task_data['id']
    name = task_data['name']

    # Run the task
    result = do_work(job_id, content, output_dir)

    # Prepare a result object
    r = { 'id' : job_id,
          'url': url,
          'name': name,
          'result' : result }
    return r

def pop_task_from_taskqueue(input_queue, output_queue, output_dir):
    '''
    '''
    task = input_queue.get()

    # PQ will return NULL after 1 second timeout if not ask
    if task is None:
        return
    
    task_output_data = run(task.data, output_dir)
    output_queue.put(task_output_data)

def main():
    ''' Single threaded worker to serve the job queue.
    '''
    # Fetch and run jobs in a loop    
    while True:
        try:
            with db_connect(os.environ['DATABASE_URL']) as conn:
                input_queue = db_queue(conn)
                output_queue = db_queue(conn, DONE_QUEUE)
                pop_task_from_taskqueue(input_queue, output_queue, _web_output_dir)
        except:
            _L.error('Error in worker main()', exc_info=True)
            time.sleep(5)

if __name__ == '__main__':
    exit(main())
