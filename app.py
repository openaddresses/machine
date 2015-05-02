from os.path import relpath, splitext, basename
from urlparse import urljoin, urlparse
from base64 import b64decode
from uuid import uuid4
import json, os

from flask import Flask, request, Response, current_app, jsonify
from uritemplate import expand
from requests import get, post
from psycopg2 import connect
from pq import PQ

app = Flask(__name__)
app.config['GITHUB_AUTH'] = os.environ['GITHUB_TOKEN'], 'x-oauth-basic'
app.config['DATABASE_URL'] = os.environ['DATABASE_URL']

MAGIC_OK_MESSAGE = 'Everything is fine'
TASK_QUEUE, DONE_QUEUE = 'tasks', 'finished'

@app.route('/')
def index():
    return 'Yo.'

@app.route('/hook', methods=['POST'])
def hook():
    github_auth = current_app.config['GITHUB_AUTH']
    webhook_payload = json.loads(request.data)
    files = process_payload_files(webhook_payload, github_auth)
    status_url = get_status_url(webhook_payload)
    
    if not files:
        update_empty_status(status_url, github_auth)
        return jsonify({'url': None, 'files': []})

    job_id = calculate_job_id(files)
    job_url = urljoin(request.url, '/jobs/{id}'.format(id=job_id))

    with db_connect(current_app) as conn:
        queue = db_queue(conn)
        with queue as db:
            try:
                # Add the touched files to a task queue.
                tasks = add_files_to_queue(queue, job_url, files)
            except Exception as e:
                # Oops, tell Github something went wrong.
                update_error_status(status_url, str(e), files.keys(), github_auth)
                return Response(json.dumps({'error': str(e), 'files': files}),
                                500, content_type='application/json')
            else:
                # That worked, remember them in the database.
                states = {name: None for name in files.keys()}
                add_job(db, job_id, tasks, states, status_url)
                update_pending_status(status_url, job_url, files.keys(), github_auth)
                return jsonify({'url': job_url, 'files': files})

@app.route('/jobs/<job_id>', methods=['GET'])
def get_job(job_id):
    '''
    '''
    with db_connect(current_app) as conn:
        with db_cursor(conn) as db:
            if read_job(db, job_id) is None:
                return Response('Job {} not found'.format(job_id), 404)
    
    return 'I am a job'

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
    ''' Return a dictionary of file paths and raw JSON contents.
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
            files[filename] = b64decode(content)
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

def add_files_to_queue(queue, job_url, files):
    ''' Make a new task for each file, return dict of taks IDs to file names.
    '''
    tasks = {}
    
    for (name, content) in files.items():
        task = queue.put(dict(url=job_url, name=name, content=content))
        
        tasks[str(task)] = name
    
    return tasks

def add_job(db, job_id, task_files, file_states, status_url):
    ''' Save information about a job to the database.
    
        Throws an IntegrityError exception if the job ID exists.
    '''
    db.execute('''INSERT INTO jobs
                  (task_files, file_states, github_status_url, id)
                  VALUES (%s::json, %s::json, %s, %s)''',
               (json.dumps(task_files), json.dumps(file_states), status_url, job_id))

def write_job(db, job_id, task_files, file_states, status_url):
    ''' Save information about a job to the database.
    '''
    db.execute('''UPDATE jobs
                  SET task_files=%s::json, file_states=%s::json, github_status_url=%s
                  WHERE id = %s''',
               (json.dumps(task_files), json.dumps(file_states), status_url, job_id))

def read_job(db, job_id):
    ''' Read information about a job from the database.
    
        Returns (task_files, file_states, github_status_url) or None.
    '''
    db.execute('''SELECT task_files, file_states, github_status_url
                  FROM jobs WHERE id = %s''', (job_id, ))
    
    try:
        filenames, states, github_status_url = db.fetchone()
    except TypeError:
        return None
    else:
        return filenames, states, github_status_url

def pop_finished_task_from_queue(queue, github_auth):
    '''
    '''
    with queue as db:
        task = queue.get()
    
        if task is None:
            return
    
        message = task.data['result']['message']
        job_url = task.data['url']
        filename = task.data['name']
        job_id = basename(urlparse(job_url).path)

        try:
            task_files, file_states, status_url = read_job(db, job_id)
        except TypeError:
            raise Exception('Job {} not found'.format(job_id))
    
        if filename not in file_states:
            raise Exception('Unknown file from job {}: "{}"'.format(job_id, filename))
        
        filenames = list(task_files.values())
        file_states[filename] = bool(message == MAGIC_OK_MESSAGE)
        
        write_job(db, job_id, task_files, file_states, status_url)
        
        if False in file_states.values():
            bad_files = [name for (name, state) in file_states.items() if state is False]
            update_failing_status(status_url, job_url, bad_files, filenames, github_auth)
        
        elif None in file_states.values():
            update_pending_status(status_url, job_url, filenames, github_auth)
        
        else:
            update_success_status(status_url, job_url, filenames, github_auth)

def db_connect(app_or_dsn):
    ''' Connect to database using Flask app instance or DSN string.
    '''
    dsn = app.config['DATABASE_URL'] if hasattr(app, 'config') else app_or_dsn
    return connect(dsn)

def db_queue(conn, name=None):
    return PQ(conn, table='queue')[name or TASK_QUEUE]

def db_cursor(conn):
    return conn.cursor()

if __name__ == '__main__':
    app.run(debug=True)
