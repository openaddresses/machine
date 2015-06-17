import logging; _L = logging.getLogger('openaddr.ci')

from ..compat import standard_library
from .. import jobs

from os.path import relpath, splitext
from urllib.parse import urljoin
from datetime import timedelta
from base64 import b64decode
from functools import wraps
from uuid import uuid4
import json, os

from flask import Flask, request, Response, current_app, jsonify
from uritemplate import expand
from requests import get, post
from psycopg2 import connect
from boto import connect_sns
from pq import PQ

def load_config():
    def truthy(value):
        return bool(value.lower() in ('yes', 'true'))

    return dict(GAG_GITHUB_STATUS=truthy(os.environ.get('GAG_GITHUB_STATUS', '')),
                GITHUB_AUTH=(os.environ['GITHUB_TOKEN'], 'x-oauth-basic'),
                DATABASE_URL=os.environ['DATABASE_URL'])

def log_application_errors(route_function):
    ''' Error-logging decorator for route functions.
    
        Don't do much, but get an error out to the logger.
    '''
    @wraps(route_function)
    def decorated_function(*args, **kwargs):
        try:
            return route_function(*args, **kwargs)
        except Exception as e:
            _L.error(e, exc_info=True)
            raise

    return decorated_function

app = Flask(__name__)
app.config.update(load_config())

MAGIC_OK_MESSAGE = 'Everything is fine'
TASK_QUEUE, DONE_QUEUE, DUE_QUEUE = 'tasks', 'finished', 'due'

# Additional delay after JOB_TIMEOUT for due tasks.
DUETASK_DELAY = timedelta(minutes=5)

@app.before_first_request
def app_prepare():
    setup_logger(os.environ.get('AWS_SNS_ARN'), logging.WARNING)

@app.route('/')
@log_application_errors
def app_index():
    return 'Yo.'

@app.route('/hook', methods=['POST'])
@log_application_errors
def app_hook():
    github_auth = current_app.config['GITHUB_AUTH']
    webhook_payload = json.loads(request.data.decode('utf8'))
    
    if webhook_payload['deleted'] is True:
        # Deleted refs will not have a status URL.
        return jsonify({'url': None, 'files': []})
    
    status_url = get_status_url(webhook_payload)
    if current_app.config['GAG_GITHUB_STATUS']:
        status_url = None
    
    try:
        files = process_payload_files(webhook_payload, github_auth)
    except Exception as e:
        message = 'Could not read source files: {}'.format(e)
        update_error_status(status_url, message, [], github_auth)
        return jsonify({'url': None, 'files': []})
    
    if not files:
        update_empty_status(status_url, github_auth)
        return jsonify({'url': None, 'files': []})

    filenames = list(files.keys())
    job_url_template = urljoin(request.url, '/jobs/{id}')

    with db_connect(current_app.config['DATABASE_URL']) as conn:
        queue = db_queue(conn, TASK_QUEUE)
        try:
            job_id = create_queued_job(queue, files, job_url_template, status_url)
            job_url = expand(job_url_template, dict(id=job_id))
        except Exception as e:
            # Oops, tell Github something went wrong.
            update_error_status(status_url, str(e), filenames, github_auth)
            return Response(json.dumps({'error': str(e), 'files': files}),
                            500, content_type='application/json')
        else:
            # That worked, tell Github we're working on it.
            update_pending_status(status_url, job_url, filenames, github_auth)
            return jsonify({'id': job_id, 'url': job_url, 'files': files})

@app.route('/jobs/<job_id>', methods=['GET'])
@log_application_errors
def app_get_job(job_id):
    '''
    '''
    with db_connect(current_app.config['DATABASE_URL']) as conn:
        with db_cursor(conn) as db:
            try:
                status, task_files, file_states, file_results, github_status_url = read_job(db, job_id)
            except TypeError:
                return Response('Job {} not found'.format(job_id), 404)
    
    job = dict(status=status, task_files=task_files, file_states=file_states,
               file_results=file_results, github_status_url=github_status_url)
    
    return jsonify(job)

def td2str(td):
    ''' Convert a timedelta to a string formatted like '3h'.
    
        Will not be necessary when https://github.com/malthe/pq/pull/5 is released.
    '''
    return '{}s'.format(td.seconds + td.days * 86400)

def get_touched_payload_files(payload):
    ''' Return a set of files modified in payload commits.
    '''
    touched = set()
    
    # Iterate over commits in chronological order.
    for commit in payload['commits']:
        for filelist in (commit['added'], commit['modified']):
            # Include any potentially-new files.
            touched.update(filelist)
        
        for filename in commit['removed']:
            # Skip files that no longer exist.
            touched.remove(filename)
        
    current_app.logger.debug('Touched files {}'.format(', '.join(touched)))
    
    return touched

def get_touched_branch_files(payload, github_auth):
    ''' Return a set of files modified between master and payload head.
    '''
    branch_sha = payload['head_commit']['id']

    compare1_url = payload['repository']['compare_url']
    compare1_url = expand(compare1_url, dict(base='master', head=branch_sha))
    current_app.logger.debug('Compare URL 1 {}'.format(compare1_url))
    
    compare1 = get(compare1_url, auth=github_auth).json()
    merge_base_sha = compare1['merge_base_commit']['sha']
    
    # That's no branch.
    if merge_base_sha == branch_sha:
        return set()

    compare2_url = payload['repository']['compare_url']
    compare2_url = expand(compare2_url, dict(base=merge_base_sha, head=branch_sha))
    current_app.logger.debug('Compare URL 2 {}'.format(compare2_url))
    
    compare2 = get(compare2_url, auth=github_auth).json()
    touched = set([file['filename'] for file in compare2['files']])
    current_app.logger.debug('Touched files {}'.format(', '.join(touched)))
    
    return touched

def process_payload_files(payload, github_auth):
    ''' Return a dictionary of file paths to raw JSON contents and file IDs.
    '''
    files = dict()

    touched = get_touched_payload_files(payload)
    touched |= get_touched_branch_files(payload, github_auth)
    
    commit_sha = payload['head_commit']['id']
    
    for filename in touched:
        if relpath(filename, 'sources').startswith('..'):
            # Skip things outside of sources directory.
            continue
        
        if splitext(filename)[1] != '.json':
            # Skip non-JSON files.
            continue
        
        contents_url = payload['repository']['contents_url']
        contents_url = expand(contents_url, dict(path=filename))
        contents_url = '{contents_url}?ref={commit_sha}'.format(**locals())
        
        current_app.logger.debug('Contents URL {}'.format(contents_url))
        
        got = get(contents_url, auth=github_auth)
        contents = got.json()
        
        if got.status_code not in range(200, 299):
            current_app.logger.warning('Skipping {} - {}'.format(filename, got.status_code))
            continue
        
        content, encoding = contents['content'], contents['encoding']
        
        current_app.logger.debug('Contents SHA {}'.format(contents['sha']))
        
        if encoding == 'base64':
            files[filename] = b64decode(content).decode('utf8'), contents['sha']
        else:
            raise ValueError('Unrecognized encoding "{}"'.format(encoding))
    
    return files

def get_status_url(payload):
    ''' Get Github status API URL from webhook payload.
    '''
    commit_sha = payload['head_commit']['id']
    status_url = payload['repository']['statuses_url']
    status_url = expand(status_url, dict(sha=commit_sha))
    
    current_app.logger.debug('Status URL {}'.format(status_url))
    
    return status_url

def post_github_status(status_url, status_json, github_auth):
    ''' POST status JSON to Github status API.
    '''
    if status_url is None:
        return
    
    # Github only wants 140 chars of description.
    status_json['description'] = status_json['description'][:140]
    
    posted = post(status_url, data=json.dumps(status_json), auth=github_auth,
                  headers={'Content-Type': 'application/json'})
    
    if posted.status_code not in range(200, 299):
        raise ValueError('Failed status post to {}'.format(status_url))
    
    if posted.json()['state'] != status_json['state']:
        raise ValueError('Mismatched status post to {}'.format(status_url))

def update_pending_status(status_url, job_url, filenames, github_auth):
    ''' Push pending status for head commit to Github status API.
    '''
    status = dict(context='openaddresses/hooked', state='pending',
                  description='Checking {}'.format(', '.join(filenames)),
                  target_url=job_url)
    
    return post_github_status(status_url, status, github_auth)

def update_error_status(status_url, message, filenames, github_auth):
    ''' Push error status for head commit to Github status API.
    '''
    status = dict(context='openaddresses/hooked', state='error',
                  description='Errored on {}: {}'.format(', '.join(filenames), message))
    
    return post_github_status(status_url, status, github_auth)

def update_failing_status(status_url, job_url, bad_files, filenames, github_auth):
    ''' Push failing status for head commit to Github status API.
    '''
    status = dict(context='openaddresses/hooked', state='failure',
                  description='Failed on {} from {}'.format(', '.join(bad_files), ', '.join(filenames)),
                  target_url=job_url)
    
    return post_github_status(status_url, status, github_auth)

def update_empty_status(status_url, github_auth):
    ''' Push success status for head commit to Github status API.
    '''
    status = dict(context='openaddresses/hooked', state='success',
                  description='Nothing to check')
    
    return post_github_status(status_url, status, github_auth)

def update_success_status(status_url, job_url, filenames, github_auth):
    ''' Push success status for head commit to Github status API.
    '''
    status = dict(context='openaddresses/hooked', state='success',
                  description='Succeeded on {}'.format(', '.join(filenames)),
                  target_url=job_url)
    
    return post_github_status(status_url, status, github_auth)

def calculate_job_id(files):
    '''
    '''
    return str(uuid4())
    
    #
    # Previously, we created a deterministic hash of
    # the files, but for now that might be too optimistic.
    #
    blob = json.dumps(files, ensure_ascii=True, sort_keys=True)
    job_id = sha1(blob).hexdigest()
    
    return job_id

def create_queued_job(queue, files, job_url_template, status_url):
    ''' Create a new job, and add its files to the queue.
    '''
    filenames = list(files.keys())
    file_states = {name: None for name in filenames}
    file_results = {name: None for name in filenames}

    job_id = calculate_job_id(files)
    job_url = job_url_template and expand(job_url_template, dict(id=job_id))
    job_status = None

    with queue as db:
        task_files = add_files_to_queue(queue, job_id, job_url, files)
        add_job(db, job_id, None, task_files, file_states, file_results, status_url)
    
    return job_id

def add_files_to_queue(queue, job_id, job_url, files):
    ''' Make a new task for each file, return dict of file IDs to file names.
    '''
    tasks = {}
    
    for (file_name, (content, file_id)) in files.items():
        task_data = dict(id=job_id, url=job_url, name=file_name,
                         content=content, file_id=file_id)
    
        queue.put(task_data, expected_at=td2str(timedelta(0)))
        tasks[file_id] = file_name
    
    return tasks

def add_job(db, job_id, status, task_files, file_states, file_results, status_url):
    ''' Save information about a job to the database.
    
        Throws an IntegrityError exception if the job ID exists.
    '''
    db.execute('''INSERT INTO jobs
                  (task_files, file_states, file_results, github_status_url, status, id)
                  VALUES (%s::json, %s::json, %s::json, %s, %s, %s)''',
               (json.dumps(task_files), json.dumps(file_states),
                json.dumps(file_results), status_url, status, job_id))

def write_job(db, job_id, status, task_files, file_states, file_results, status_url):
    ''' Save information about a job to the database.
    '''
    db.execute('''UPDATE jobs
                  SET task_files=%s::json, file_states=%s::json,
                      file_results=%s::json, github_status_url=%s, status=%s
                  WHERE id = %s''',
               (json.dumps(task_files), json.dumps(file_states),
                json.dumps(file_results), status_url, status, job_id))

def read_job(db, job_id):
    ''' Read information about a job from the database.
    
        Returns (status, task_files, file_states, file_results, github_status_url) or None.
    '''
    db.execute('''SELECT status, task_files, file_states, file_results, github_status_url
                  FROM jobs WHERE id = %s''', (job_id, ))
    
    try:
        status, task_files, states, file_results, github_status_url = db.fetchone()
    except TypeError:
        return None
    else:
        return status, task_files, states, file_results, github_status_url

def is_completed_run(db, file_id, min_datetime):
    '''
    '''
    db.execute('''SELECT id FROM runs
                  WHERE source_id = %s
                    AND datetime >= %s''',
               (file_id, min_datetime))
    
    completed_run = db.fetchone()
    
    return bool(completed_run is not None)

def add_run(db, filename, file_id, content, run_state):
    '''
    '''
    db.execute('''INSERT INTO runs
                  (source_path, source_data, source_id, state, datetime)
                  VALUES (%s, %s, %s, %s::json, NOW() AT TIME ZONE 'UTC')''',
               (filename, content, file_id, json.dumps(run_state)))

def update_job_status(db, job_id, job_url, filenames, task_files, file_states, file_results, status_url, github_auth):
    '''
    '''
    if False in file_states.values():
        # Any task failure means the whole job has failed.
        job_status = False
    elif None in file_states.values():
        job_status = None
    else:
        job_status = True
    
    write_job(db, job_id, job_status, task_files, file_states, file_results, status_url)
    
    if not status_url:
        _L.warning('No status_url to tell about {} status of job {}'.format(job_status, job_id))
        return
    
    if job_status is False:
        bad_files = [name for (name, state) in file_states.items() if state is False]
        update_failing_status(status_url, job_url, bad_files, filenames, github_auth)
    
    elif job_status is None:
        update_pending_status(status_url, job_url, filenames, github_auth)
    
    elif job_status is True:
        update_success_status(status_url, job_url, filenames, github_auth)

def pop_task_from_taskqueue(task_queue, done_queue, due_queue, output_dir):
    '''
    '''
    with task_queue as db:
        task = task_queue.get()

        # PQ will return NULL after 1 second timeout if not ask
        if task is None:
            return
    
        _L.info('Got job {} from task queue'.format(task.data['id']))
        passed_on_task_keys = 'id', 'file_id', 'name', 'url', 'content'
        passed_on_task_kwargs = {k: task.data.get(k) for k in passed_on_task_keys}

        # Send a Due task, possibly for later.
        due_task_data = dict(task_data=task.data, **passed_on_task_kwargs)
        due_queue.put(due_task_data, schedule_at=td2str(jobs.JOB_TIMEOUT + DUETASK_DELAY))

    # Run the task.
    from . import worker # <-- TODO: un-suck this.
    result = worker.do_work(task.data['content'], output_dir)

    # Send a Done task
    done_task_data = dict(result=result, **passed_on_task_kwargs)
    done_queue.put(done_task_data, expected_at=td2str(timedelta(0)))

def pop_task_from_donequeue(queue, github_auth):
    ''' Look for a completed job in the "done" task queue, update Github status.
    '''
    with queue as db:
        task = queue.get()
    
        if task is None:
            return
    
        _L.info('Got job {} from done queue'.format(task.data['id']))
        results = task.data['result']
        message = results['message']
        run_state = results.get('output', None)
        content = task.data['content']
        job_url = task.data['url']
        filename = task.data['name']
        file_id = task.data['file_id']
        job_id = task.data['id']
        
        if is_completed_run(db, file_id, task.enqueued_at):
            # We are too late, this got handled.
            return
        
        add_run(db, filename, file_id, content, run_state)

        try:
            _, task_files, file_states, file_results, status_url = read_job(db, job_id)
        except TypeError:
            raise Exception('Job {} not found'.format(job_id))
    
        if filename not in file_states:
            raise Exception('Unknown file from job {}: "{}"'.format(job_id, filename))
        
        filenames = list(task_files.values())
        file_states[filename] = bool(message == MAGIC_OK_MESSAGE)
        file_results[filename] = results
        
        update_job_status(db, job_id, job_url, filenames, task_files,
                          file_states, file_results, status_url, github_auth)

def pop_task_from_duequeue(queue, github_auth):
    '''
    '''
    with queue as db:
        task = queue.get()
    
        if task is None:
            return
        
        _L.info('Got job {} from due queue'.format(task.data['id']))
        original_task = task.data['task_data']
        content = task.data['content']
        job_url = task.data['url']
        filename = task.data['name']
        file_id = task.data['file_id']
        job_id = task.data['id']
    
        if is_completed_run(db, file_id, task.enqueued_at):
            # Everything's fine, this got handled.
            return

        add_run(db, filename, file_id, content, None)

        try:
            _, task_files, file_states, file_results, status_url = read_job(db, job_id)
        except TypeError:
            raise Exception('Job {} not found'.format(job_id))
    
        if filename not in file_states:
            raise Exception('Unknown file from job {}: "{}"'.format(job_id, filename))
        
        filenames = list(task_files.values())
        file_states[filename] = False
        file_results[filename] = False
        
        update_job_status(db, job_id, job_url, filenames, task_files,
                          file_states, file_results, status_url, github_auth)

def db_connect(dsn):
    ''' Connect to database using DSN string.
    '''
    return connect(dsn)

def db_queue(conn, name):
    return PQ(conn, table='queue')[name]

def db_cursor(conn):
    return conn.cursor()

class SnsHandler(logging.Handler):
    ''' Logs to the given Amazon SNS topic; meant for errors.
    '''
    def __init__(self, arn, *args, **kwargs):
        super(SnsHandler, self).__init__(*args, **kwargs)
        
        # Rely on boto AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY variables.
        self.arn, self.sns = arn, connect_sns()

    def emit(self, record):
        subject = u'OpenAddr: {}: {}'.format(record.levelname, record.name)
        self.sns.publish(self.arn, self.format(record), subject[:79])

def setup_logger(sns_arn, log_level=logging.DEBUG):
    ''' Set up logging for openaddr code.
    '''
    # Get a handle for the openaddr logger and its children
    openaddr_logger = logging.getLogger('openaddr')

    # Default logging format.
    log_format = '%(asctime)s %(levelname)07s: %(message)s'

    # Set the logger level to show everything, and filter down in the handlers.
    openaddr_logger.setLevel(log_level)

    # Set up a logger to stderr
    handler1 = logging.StreamHandler()
    handler1.setLevel(log_level)
    handler1.setFormatter(logging.Formatter(log_format))
    openaddr_logger.addHandler(handler1)
    
    # Set up a second logger to SNS
    try:
        handler2 = SnsHandler(sns_arn)
    except:
        openaddr_logger.warning('Failed to authenticate SNS handler')
    else:
        handler2.setLevel(logging.ERROR)
        handler2.setFormatter(logging.Formatter(log_format))
        openaddr_logger.addHandler(handler2)

if __name__ == '__main__':
    app.run(debug=True)
