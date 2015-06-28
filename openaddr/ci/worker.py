#!/usr/bin/env python2
'''
Simple worker process to process OpenAddress sources on the task queue

Jobs get enqueued to a PQ task queue by some other system.
This program pops jobs and runs them one at a time, then
enqueues a new message on a separate PQ queue when the work is done.
'''
import logging; _L = logging.getLogger('openaddr.ci.worker')

from .. import compat, S3, package_output
from ..jobs import JOB_TIMEOUT

import time, os, psycopg2, json, tempfile, shutil, base64
from urllib.parse import urlparse, urljoin

from . import (
    db_connect, db_queue, db_queue, pop_task_from_taskqueue,
    MAGIC_OK_MESSAGE, DONE_QUEUE, TASK_QUEUE, DUE_QUEUE, setup_logger
    )

def upload_file(s3, keyname, filename):
    ''' Create a new S3 key with filename contents, return its URL and MD5 hash.
    '''
    key = s3.new_key(keyname)

    kwargs = dict(policy='public-read', reduced_redundancy=True)
    key.set_contents_from_filename(filename, **kwargs)
    url = key.generate_url(expires_in=0, query_auth=False, force_http=True)
    
    return url, key.md5

def do_work(s3, run_id, source_name, job_contents_b64, output_dir):
    "Do the actual work of running a source file in job_contents"

    # Make a directory to run the whole job
    workdir = tempfile.mkdtemp(prefix='work-', dir=output_dir)

    # Write the user input to a file
    out_fn = os.path.join(workdir, 'user_input.txt')
    with open(out_fn, 'wb') as out_fp:
        out_fp.write(base64.b64decode(job_contents_b64))

    # Make a directory in which to run openaddr
    oa_dir = os.path.join(workdir, 'out')
    os.mkdir(oa_dir)

    # Invoke the job to do
    cmd = 'openaddr-process-one', '-l', os.path.join(workdir, 'logfile.txt'), out_fn, oa_dir
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

    with open(state_fullpath) as file:
        index = dict(zip(*json.load(file)))
        
        for key in ('processed', 'sample', 'cache'):
            if not index[key]:
                result.update(result_code=-1, message='Failed to produce {} data'.format(key))
        
        index_dirname = os.path.dirname(state_fullpath)
        
        if index['cache']:
            # e.g. /runs/0/cache.zip
            cache_path = os.path.join(index_dirname, index['cache'])
            key_name = '/runs/{run}/{cache}'.format(run=run_id, **index)
            url, fingerprint = upload_file(s3, key_name, cache_path)
            index['cache'], index['fingerprint'] = url, fingerprint
        
        if index['sample']:
            # e.g. /runs/0/sample.json
            sample_path = os.path.join(index_dirname, index['sample'])
            key_name = '/runs/{run}/{sample}'.format(run=run_id, **index)
            url, _ = upload_file(s3, key_name, sample_path)
            index['sample'] = url
        
        if index['processed']:
            # e.g. /runs/0/fr/paris.zip
            processed_path = os.path.join(index_dirname, index['processed'])
            archive_path = package_output(source_name, processed_path)
            key_name = u'/runs/{run}/{name}.zip'.format(run=run_id, name=source_name)
            url, _ = upload_file(s3, key_name, archive_path)
            index['processed'] = url
            os.remove(archive_path)
        
        if index['output']:
            # e.g. /runs/0/output.txt
            output_path = os.path.join(index_dirname, index['output'])
            key_name = '/runs/{run}/{output}'.format(run=run_id, **index)
            url, _ = upload_file(s3, key_name, output_path)
            index['output'] = url
        
        result['output'] = index
    
    shutil.rmtree(workdir)
    return result

def main():
    ''' Single threaded worker to serve the job queue.
    '''
    setup_logger(os.environ.get('AWS_SNS_ARN'))

    # Rely on boto AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY variables.
    s3 = S3(None, None, os.environ.get('AWS_S3_BUCKET', 'data.openaddresses.io'))

    # Fetch and run jobs in a loop    
    while True:
        try:
            with db_connect(os.environ['DATABASE_URL']) as conn:
                task_Q = db_queue(conn, TASK_QUEUE)
                done_Q = db_queue(conn, DONE_QUEUE)
                due_Q = db_queue(conn, DUE_QUEUE)
                pop_task_from_taskqueue(s3, task_Q, done_Q, due_Q, tempfile.gettempdir())
        except:
            _L.error('Error in worker main()', exc_info=True)
            time.sleep(5)

if __name__ == '__main__':
    exit(main())
