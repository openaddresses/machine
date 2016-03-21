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

from argparse import ArgumentParser
import time, os, psycopg2, json, tempfile, shutil, base64
from urllib.parse import urlparse, urljoin

from . import (
    db_connect, db_queue, db_queue, pop_task_from_taskqueue,
    MAGIC_OK_MESSAGE, DONE_QUEUE, TASK_QUEUE, DUE_QUEUE, setup_logger,
    log_function_errors, HEARTBEAT_QUEUE
    )

def upload_file(s3, keyname, filename):
    ''' Create a new S3 key with filename contents, return its URL and MD5 hash.
    '''
    key = s3.new_key(keyname)

    kwargs = dict(policy='public-read', reduced_redundancy=True)
    key.set_contents_from_filename(filename, **kwargs)
    url = key.generate_url(expires_in=0, query_auth=False, force_http=True)
    
    return url, key.md5

def make_source_filename(source_name):
    '''
    '''
    return source_name.replace(u'/', u'--') + '.txt'

def do_work(s3, run_id, source_name, job_contents_b64, output_dir):
    "Do the actual work of running a source file in job_contents"

    # Make a directory to run the whole job
    workdir = tempfile.mkdtemp(prefix='work-', dir=output_dir)

    # Write the user input to a file
    out_fn = os.path.join(workdir, make_source_filename(source_name))
    with open(out_fn, 'wb') as out_fp:
        out_fp.write(base64.b64decode(job_contents_b64))

    # Make a directory in which to run openaddr
    oa_dir = os.path.join(workdir, 'out')
    os.mkdir(oa_dir)

    # Invoke the job to do
    logfile_path = os.path.join(workdir, 'logfile.txt')
    cmd = 'openaddr-process-one', '-l', logfile_path, out_fn, oa_dir
    try:
        known_error, cmd_status = False, 0
        timeout_seconds = JOB_TIMEOUT.seconds + JOB_TIMEOUT.days * 86400
        result_stdout = compat.check_output(cmd, timeout=timeout_seconds)
    except compat.TimeoutExpired as e:
        known_error, cmd_status, result_stdout = True, None, getattr(e, 'stdout', None)
    except compat.CalledProcessError as e:
        known_error, cmd_status, result_stdout = True, e.returncode, e.output
    except Exception:
        known_error, cmd_status = False, None
        raise
    else:
        if hasattr(result_stdout, 'decode'):
            # "The actual encoding of the output data may depend on the command
            # being invoked" - https://docs.python.org/3/library/subprocess.html
            result_stdout = result_stdout.decode('utf8', 'replace')
    finally:
        if known_error:
            # Something went wrong; throw back an error result.
            key_name = '/runs/{run}/logfile.txt'.format(run=run_id)
            try:
                url, _ = upload_file(s3, key_name, logfile_path)
            except IOError:
                output = dict()
            else:
                output = dict(output=url)

            return dict(result_code=cmd_status, result_stdout=result_stdout,
                        message='Something went wrong in {0}'.format(*cmd),
                        output=output)

    result = dict(result_code=0, result_stdout=result_stdout,
                  message=MAGIC_OK_MESSAGE)

    # openaddr-process-one prints a path to index.json
    state_fullpath = result_stdout.strip()

    with open(state_fullpath) as file:
        index = dict(zip(*json.load(file)))
        
        for key in ('processed', 'sample', 'cache'):
            if not index[key] and not index.get('skipped'):
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
            package_args = index.get('website') or 'Unknown', index.get('license') or 'Unknown'
            archive_path = package_output(source_name, processed_path, *package_args)
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

parser = ArgumentParser(description='Run some source files.')

parser.add_argument('-b', '--bucket', default='data.openaddresses.io',
                    help='S3 bucket name. Defaults to "data.openaddresses.io".')

parser.add_argument('-d', '--database-url', default=os.environ.get('DATABASE_URL', None),
                    help='Optional connection string for database. Defaults to value of DATABASE_URL environment variable.')

parser.add_argument('-a', '--access-key', default=os.environ.get('AWS_ACCESS_KEY_ID', None),
                    help='Optional AWS access key name. Defaults to value of AWS_ACCESS_KEY_ID environment variable.')

parser.add_argument('-s', '--secret-key', default=os.environ.get('AWS_SECRET_ACCESS_KEY', None),
                    help='Optional AWS secret key name. Defaults to value of AWS_SECRET_ACCESS_KEY environment variable.')

parser.add_argument('--sns-arn', default=os.environ.get('AWS_SNS_ARN', None),
                    help='Optional AWS Simple Notification Service (SNS) resource. Defaults to value of AWS_SNS_ARN environment variable.')

parser.add_argument('-v', '--verbose', help='Turn on verbose logging',
                    action='store_const', dest='loglevel',
                    const=logging.DEBUG, default=logging.INFO)

parser.add_argument('-q', '--quiet', help='Turn off most logging',
                    action='store_const', dest='loglevel',
                    const=logging.WARNING, default=logging.INFO)

worker_kind = os.environ.get('WORKER_KIND')

@log_function_errors
def main():
    ''' Single threaded worker to serve the job queue.
    '''
    args = parser.parse_args()
    setup_logger(args.sns_arn, log_level=args.loglevel)
    s3 = S3(args.access_key, args.secret_key, args.bucket)
    
    # Fetch and run jobs in a loop    
    while True:
        worker_dir = tempfile.mkdtemp(prefix='worker-')
    
        try:
            with db_connect(args.database_url) as conn:
                task_Q = db_queue(conn, TASK_QUEUE)
                done_Q = db_queue(conn, DONE_QUEUE)
                due_Q = db_queue(conn, DUE_QUEUE)
                beat_Q = db_queue(conn, HEARTBEAT_QUEUE)
                pop_task_from_taskqueue(s3, task_Q, done_Q, due_Q, beat_Q, worker_dir, worker_kind)
        except:
            _L.error('Error in worker main()', exc_info=True)
            time.sleep(5)
        finally:
            shutil.rmtree(worker_dir)

if __name__ == '__main__':
    exit(main())
