#!/usr/bin/env python2

"""Simple worker process to process OpenAddress sources on the task queue

Jobs get enqueued to a PQ task queue by some other system.
This program pops jobs and runs them one at a time, then
enqueues a new message on a separate PQ queue when the work is done."""

import time, os, subprocess, psycopg2
import lib

# File path and URL path for result directory. Should be S3.
_web_output_dir = '/var/www/html/oa-runone'
_web_base_url = 'http://minar.us.to/oa-runone'

def do_work(job_id, job_contents):
    "Do the actual work of running a source file in job_contents"

    # Make a directory to run the whole job
    assert '/' not in job_id
    out_dir = '%s/%s' % (_web_output_dir, job_id)
    os.mkdir(out_dir)

    # Write the user input to a file
    out_fn = '%s/user_input.txt' % out_dir
    with file(out_fn, 'wb') as out_fp:
        out_fp.write(job_contents)

    # Make a directory in which to run openaddr
    oa_dir = '%s/%s' % (out_dir, 'out')
    os.mkdir(oa_dir)

    # Invoke the job to do
    result_code = subprocess.call(('openaddr-process-one',
                                   '-l', '%s/logfile.txt' % out_dir,
                                   out_fn,
                                   oa_dir))

    # Prepare return parameters
    r = { 'result_code': result_code,
          'output_url': '%s/%s' % (_web_base_url, job_id),
          'message': 'A Zircon princess seemed to lost her senses' }

    return r

def run(task_data):
    "Run a task posted to the queue. Handles the JSON for task and result objects."

    print "Got job %s" % task_data

    # Unpack the task object
    content = task_data['content']
    url = task_data['url']
    job_id = task_data['id']
    name = task_data['name']

    # Run the task
    result = do_work(job_id, content)

    # Prepare a result object
    r = { 'id' : job_id,
          'url': url,
          'name': name,
          'result' : result }
    return r

def serve_queue():
    "Single threaded worker to serve the job queue"

    # Connect to the queue
    conn = lib.db_connect(os.environ['DATABASE_STRING'])
    input_queue = lib.db_queue(conn)
    output_queue = lib.db_queue(conn, lib.DONE_QUEUE)

    # Fetch and run jobs in a loop    
    while True:
        task = input_queue.get()
        # PQ will return NULL after 1 second timeout if not ask
        if task:
            task_output_data = run(task.data)
            output_queue.put(task_output_data)


if __name__ == '__main__':
    serve_queue()
