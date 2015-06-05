import logging; _L = logging.getLogger('openaddr.ci')

from ..compat import standard_library
from .. import jobs

from os.path import relpath, splitext
from urllib.parse import urljoin
from datetime import timedelta
from base64 import b64decode
from uuid import uuid4
import json, os

from flask import Flask, request, Response, current_app, jsonify
from uritemplate import expand
from requests import get, post
from psycopg2 import connect
from pq import PQ

def load_config():
    return dict(GITHUB_AUTH=(os.environ['GITHUB_TOKEN'], 'x-oauth-basic'),
                DATABASE_URL=os.environ['DATABASE_URL'])

app = Flask(__name__)
app.config.update(load_config())

MAGIC_OK_MESSAGE = 'Everything is fine'
TASK_QUEUE, DONE_QUEUE, DUE_QUEUE = 'tasks', 'finished', 'due'

@app.route('/')
def index():
    return 'Yo.'

@app.route('/hook', methods=['POST'])
def hook():
    github_auth = current_app.config['GITHUB_AUTH']
    webhook_payload = json.loads(request.data.decode('utf8'))
    files = process_payload_files(webhook_payload, github_auth)
    status_url = get_status_url(webhook_payload)
    
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
def get_job(job_id):
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

def pop_task_from_taskqueue(task_queue, done_queue, due_queue, output_dir):
    '''
    '''
    with task_queue as db:
        task = task_queue.get()

        # PQ will return NULL after 1 second timeout if not ask
        if task is None:
            return
    
    _L.info("Got job {}".format(task.data))

    # Send a Due task, possibly for later.
    job_id, file_id = task.data['id'], task.data['file_id']
    due_task_data = dict(task_data=task.data, id=job_id, file_id=file_id)
    due_queue.put(due_task_data, schedule_at=td2str(jobs.JOB_TIMEOUT))

    # Run the task.
    from . import worker # <-- TODO: un-suck this.
    result = worker.do_work(task.data['id'], task.data['content'], output_dir)

    # Send a Done task
    done_task_data = {k: task.data[k] for k in ('id', 'url', 'name', 'file_id')}
    done_task_data['result'] = result
    done_queue.put(done_task_data, expected_at=td2str(timedelta(0)))

def pop_task_from_donequeue(queue, github_auth):
    ''' Look for a completed job in the "done" task queue, update Github status.
    '''
    with queue as db:
        task = queue.get()
    
        if task is None:
            return
    
        results = task.data['result']
        message = results['message']
        run_state = results.get('output', None)
        job_url = task.data['url']
        filename = task.data['name']
        file_id = task.data['file_id']
        job_id = task.data['id']
        
        #
        # Add to the runs table.
        #
        db.execute('''INSERT INTO runs
                      (source_path, source_id, state, datetime)
                      VALUES (%s, %s, %s::json, NOW() AT TIME ZONE 'UTC')''',
                   (filename, file_id, json.dumps(run_state)))

        try:
            _, task_files, file_states, file_results, status_url = read_job(db, job_id)
        except TypeError:
            raise Exception('Job {} not found'.format(job_id))
    
        if filename not in file_states:
            raise Exception('Unknown file from job {}: "{}"'.format(job_id, filename))
        
        filenames = list(task_files.values())
        file_states[filename] = bool(message == MAGIC_OK_MESSAGE)
        file_results[filename] = results
        
        if False in file_states.values():
            # Any task failure means the whole job has failed.
            job_status = False
        elif None in file_states.values():
            job_status = None
        else:
            job_status = True
        
        write_job(db, job_id, job_status, task_files, file_states, file_results, status_url)
        
        if not status_url:
            return
        
        if job_status is False:
            bad_files = [name for (name, state) in file_states.items() if state is False]
            update_failing_status(status_url, job_url, bad_files, filenames, github_auth)
        
        elif job_status is None:
            update_pending_status(status_url, job_url, filenames, github_auth)
        
        elif job_status is True:
            update_success_status(status_url, job_url, filenames, github_auth)

def pop_task_from_duequeue(queue):
    '''
    '''
    with queue as db:
        task = queue.get()
    
        if task is None:
            return
    
        db.execute('''SELECT id FROM runs
                      WHERE source_id = %s
                        AND datetime >= %s''',
                   (task.data['file_id'], task.enqueued_at))
        
        completed_run = db.fetchone()
        
        if completed_run is not None:
            # Everything's fine, this got handled.
            return
        
        # No run was completed, so this due task represents a failure.
        raise NotImplementedError('Need to write this.')

def db_connect(dsn):
    ''' Connect to database using DSN string.
    '''
    return connect(dsn)

def db_queue(conn, name):
    return PQ(conn, table='queue')[name]

def db_cursor(conn):
    return conn.cursor()

if __name__ == '__main__':
    app.run(debug=True)
