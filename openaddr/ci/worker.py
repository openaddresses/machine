#!/usr/bin/env python2

"""Simple worker process to process OpenAddress sources on the task queue

Jobs get enqueued to a PQ task queue by some other system.
This program pops jobs and runs them one at a time, then
enqueues a new message on a separate PQ queue when the work is done."""

import time, os, subprocess, psycopg2, urlparse, socket

from . import db_connect, db_queue, db_queue, MAGIC_OK_MESSAGE, DONE_QUEUE

# File path and URL path for result directory. Should be S3.
_web_output_dir = '/var/www/html/oa-runone'
_web_base_url = 'http://{host}/oa-runone/'.format(host=socket.getfqdn())

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
    cmd = 'openaddr-process-one', '-l', '%s/logfile.txt' % out_dir, out_fn, oa_dir
    try:
        result_stdout = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        message = 'Something went wrong in {0}'.format(*cmd)
        result_code, result_stdout = e.returncode, None
    else:
        result_code, message = 0, MAGIC_OK_MESSAGE

    # Prepare return parameters
    r = { 'result_code': result_code,
          'result_stdout': result_stdout,
          'output_url': urlparse.urljoin(_web_base_url, job_id),
          'message': message }

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

def main():
    "Single threaded worker to serve the job queue"

    # Connect to the queue
    conn = db_connect(os.environ['DATABASE_URL'])
    input_queue = db_queue(conn)
    output_queue = db_queue(conn, DONE_QUEUE)

    # Fetch and run jobs in a loop    
    while True:
        task = input_queue.get()
        # PQ will return NULL after 1 second timeout if not ask
        if task:
            task_output_data = run(task.data)
            output_queue.put(task_output_data)


if __name__ == '__main__':
    exit(main())
