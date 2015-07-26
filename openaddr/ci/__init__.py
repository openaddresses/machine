import logging; _L = logging.getLogger('openaddr.ci')

from ..compat import standard_library
from .. import jobs, render, __version__

from os.path import relpath, splitext, join, basename
from datetime import timedelta
from uuid import uuid4, getnode
from base64 import b64decode
from tempfile import mkdtemp
from shutil import rmtree
from time import sleep
import json, os

from flask import Flask, request, Response, current_app, jsonify, render_template
from uritemplate import expand
from requests import get, post
from dateutil.tz import tzutc
from psycopg2 import connect
from boto import connect_sns
from pq import PQ

# Ask Python 2 to get real unicode from the database.
# http://initd.org/psycopg/docs/usage.html#unicode-handling
import psycopg2.extensions
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)

def load_config():
    def truthy(value):
        return bool(value.lower() in ('yes', 'true'))
    
    secrets_string = os.environ.get('WEBHOOK_SECRETS', u'').encode('utf8')
    webhook_secrets = secrets_string.split(b',') if secrets_string else []

    return dict(GAG_GITHUB_STATUS=truthy(os.environ.get('GAG_GITHUB_STATUS', '')),
                GITHUB_AUTH=(os.environ['GITHUB_TOKEN'], 'x-oauth-basic'),
                DATABASE_URL=os.environ['DATABASE_URL'],
                WEBHOOK_SECRETS=webhook_secrets)

MAGIC_OK_MESSAGE = 'Everything is fine'
TASK_QUEUE, DONE_QUEUE, DUE_QUEUE = 'tasks', 'finished', 'due'

# Additional delay after JOB_TIMEOUT for due tasks.
DUETASK_DELAY = timedelta(minutes=5)

# Amount of time to reuse run results.
RUN_REUSE_TIMEOUT = timedelta(days=5)

# Time to chill out in pop_task_from_taskqueue() after sending Done task.
WORKER_COOLDOWN = timedelta(seconds=5)

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
            if filename in touched:
                touched.remove(filename)
        
    current_app.logger.debug(u'Touched files {}'.format(', '.join(touched)))
    
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
    current_app.logger.debug(u'Touched files {}'.format(', '.join(touched)))
    
    return touched

def get_touched_pullrequest_files(payload, github_auth):
    ''' Return a set of files modified between master and payload head.
    '''
    if payload['action'] == 'closed':
        return set()
    
    base_sha = payload['pull_request']['base']['sha']
    head_sha = payload['pull_request']['head']['sha']

    compare_url = payload['pull_request']['head']['repo']['compare_url']
    compare_url = expand(compare_url, dict(head=head_sha, base=base_sha))
    current_app.logger.debug('Compare URL {}'.format(compare_url))
    
    compare = get(compare_url, auth=github_auth).json()
    touched = set([file['filename'] for file in compare['files']])
    current_app.logger.debug(u'Touched files {}'.format(', '.join(touched)))
    
    return touched

def skip_payload(payload):
    ''' Return True if this payload should not be processed.
    '''
    if 'action' in payload and 'pull_request' in payload:
        return bool(payload['action'] == 'closed')
    
    if 'commits' in payload and 'head_commit' in payload:
        # Deleted refs will not have a status URL.
        return bool(payload.get('deleted') == True)
    
    return True

def process_payload_files(payload, github_auth):
    ''' Return a dictionary of file paths to raw JSON contents and file IDs.
    '''
    if 'action' in payload and 'pull_request' in payload:
        return process_pullrequest_payload_files(payload, github_auth)
    
    if 'commits' in payload and 'head_commit' in payload:
        return process_pushevent_payload_files(payload, github_auth)
    
    raise ValueError('Unintelligible webhook payload')

def process_pullrequest_payload_files(payload, github_auth):
    ''' Return a dictionary of files paths from a pull request event payload.
    
        https://developer.github.com/v3/activity/events/types/#pullrequestevent
    '''
    files = dict()
    touched = get_touched_pullrequest_files(payload, github_auth)
    
    commit_sha = payload['pull_request']['head']['sha']
    
    for filename in touched:
        if relpath(filename, 'sources').startswith('..'):
            # Skip things outside of sources directory.
            continue
        
        if splitext(filename)[1] != '.json':
            # Skip non-JSON files.
            continue

        contents_url = payload['pull_request']['head']['repo']['contents_url'] + '{?ref}'
        try:
            contents_url = expand(contents_url, dict(path=filename, ref=commit_sha))
        except UnicodeEncodeError:
            # Python 2 behavior
            contents_url = expand(contents_url, dict(path=filename.encode('utf8'), ref=commit_sha))
        
        current_app.logger.debug('Contents URL {}'.format(contents_url))
        
        got = get(contents_url, auth=github_auth)
        contents = got.json()
        
        if got.status_code not in range(200, 299):
            current_app.logger.warning('Skipping {} - {}'.format(filename, got.status_code))
            continue
        
        if contents['encoding'] != 'base64':
            raise ValueError('Unrecognized encoding "{encoding}"'.format(**contents))
        
        current_app.logger.debug('Contents SHA {sha}'.format(**contents))
        files[filename] = contents['content'], contents['sha']
    
    return files

def process_pushevent_payload_files(payload, github_auth):
    ''' Return a dictionary of files paths from a push event payload.
    
        https://developer.github.com/v3/activity/events/types/#pushevent
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
        
        contents_url = payload['repository']['contents_url'] + '{?ref}'
        try:
            contents_url = expand(contents_url, dict(path=filename, ref=commit_sha))
        except UnicodeEncodeError:
            # Python 2 behavior
            contents_url = expand(contents_url, dict(path=filename.encode('utf8'), ref=commit_sha))
        
        current_app.logger.debug('Contents URL {}'.format(contents_url))
        
        got = get(contents_url, auth=github_auth)
        contents = got.json()
        
        if got.status_code not in range(200, 299):
            current_app.logger.warning('Skipping {} - {}'.format(filename, got.status_code))
            continue
        
        if contents['encoding'] != 'base64':
            raise ValueError('Unrecognized encoding "{encoding}"'.format(**contents))
        
        current_app.logger.debug('Contents SHA {sha}'.format(**contents))
        files[filename] = contents['content'], contents['sha']
    
    return files

def get_commit_info(payload):
    ''' Get commit SHA and Github status API URL from webhook payload.
    '''
    if 'pull_request' in payload:
        commit_sha = payload['pull_request']['head']['sha']
        status_url = payload['pull_request']['statuses_url']
    
    elif 'head_commit' in payload:
        commit_sha = payload['head_commit']['id']
        status_url = payload['repository']['statuses_url']
        status_url = expand(status_url, dict(sha=commit_sha))
    
    else:
        raise ValueError('Unintelligible payload')
    
    current_app.logger.debug('Status URL {}'.format(status_url))
    
    return commit_sha, status_url

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
                  description=u'Checking {}'.format(', '.join(filenames)),
                  target_url=job_url)
    
    return post_github_status(status_url, status, github_auth)

def update_error_status(status_url, message, filenames, github_auth):
    ''' Push error status for head commit to Github status API.
    '''
    status = dict(context='openaddresses/hooked', state='error',
                  description=u'Errored on {}: {}'.format(', '.join(filenames), message))
    
    return post_github_status(status_url, status, github_auth)

def update_failing_status(status_url, job_url, bad_files, filenames, github_auth):
    ''' Push failing status for head commit to Github status API.
    '''
    status = dict(context='openaddresses/hooked', state='failure',
                  description=u'Failed on {} from {}'.format(', '.join(bad_files), ', '.join(filenames)),
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
                  description=u'Succeeded on {}'.format(', '.join(filenames)),
                  target_url=job_url)
    
    return post_github_status(status_url, status, github_auth)

def find_batch_sources(owner, repository, github_auth):
    ''' Starting with a Github repo API URL, generate a stream of master sources.
    '''
    resp = get('https://api.github.com/', auth=github_auth)
    if resp.status_code >= 400:
        raise Exception('Got status {} from Github API'.format(resp.status_code))
    start_url = expand(resp.json()['repository_url'], dict(owner=owner, repo=repository))
    
    _L.info('Starting batch sources at {start_url}'.format(**locals()))
    got = get(start_url, auth=github_auth).json()
    contents_url, commits_url = got['contents_url'], got['commits_url']

    master_url = expand(commits_url, dict(sha=got['default_branch']))

    _L.debug('Getting {ref} branch {master_url}'.format(ref=got['default_branch'], **locals()))
    got = get(master_url, auth=github_auth).json()
    commit_sha, commit_date = got['sha'], got['commit']['committer']['date']
    
    contents_url += '{?ref}' # So that we are consistently at the same commit.
    sources_urls = [expand(contents_url, dict(path='sources', ref=commit_sha))]
    sources_dict = dict()

    for sources_url in sources_urls:
        _L.debug('Getting sources {sources_url}'.format(**locals()))
        sources = get(sources_url, auth=github_auth).json()
    
        for source in sources:
            if source['type'] == 'dir':
                params = dict(path=source['path'], ref=commit_sha)
                sources_urls.append(expand(contents_url, params))
                continue
        
            if source['type'] != 'file':
                continue
        
            path_base, ext = splitext(source['path'])
        
            if ext == '.json':
                _L.debug('Getting source {url}'.format(**source))
                more_source = get(source['url'], auth=github_auth).json()

                yield dict(commit_sha=commit_sha, url=source['url'],
                           blob_sha=source['sha'], path=source['path'],
                           content=more_source['content'])

def enqueue_sources(queue, the_set, sources):
    ''' Batch task generator, yields counts of remaining expected paths.
    '''
    expected_paths = set()
    commit_sha = None
    
    #
    # Enqueue each source if there is nothing else in the queue.
    #
    for source in sources:
        while len(queue) >= 1:
            yield len(expected_paths)
        
        with queue as db:
            _L.info(u'Sending {path} to task queue'.format(**source))
            task_data = dict(job_id=None, url=None, set_id=the_set.id,
                             name=source['path'],
                             content_b64=source['content'],
                             commit_sha=source['commit_sha'],
                             file_id=source['blob_sha'])
        
            task_id = queue.put(task_data)
            expected_paths.add(source['path'])
            commit_sha = source['commit_sha']
    
    while len(expected_paths):
        with queue as db:
            _update_expected_paths(db, expected_paths, the_set)

        yield len(expected_paths)

    with queue as db:
        _L.info(u'Updating set {} in sets table'.format(the_set.id))
        db.execute('''UPDATE sets
                      SET datetime_end = NOW(), commit_sha = %s
                      WHERE id = %s''',
                   (commit_sha, the_set.id))

    yield 0

def _update_expected_paths(db, expected_paths, the_set):
    ''' Discard sources from expected_paths set as they appear in runs table.
    '''
    db.execute('''SELECT source_path FROM runs
                  WHERE set_id = %s AND status IS NOT NULL''',
               (the_set.id, ))

    for (source_path, ) in db:
        _L.debug(u'Discarding {}'.format(source_path))
        expected_paths.discard(source_path)

def render_set_maps(s3, db, the_set):
    ''' Render set maps, upload them to S3 and add to the database.
    '''
    dirname = mkdtemp(prefix='set-maps-')

    try:
        good_sources = _prepare_render_sources(db, the_set, dirname)

        urls = dict()
        areas = (render.WORLD, 'world'), (render.USA, 'usa'), (render.EUROPE, 'europe')
        key_kwargs = dict(policy='public-read', headers={'Content-Type': 'image/png'})
        url_kwargs = dict(expires_in=0, query_auth=False, force_http=True)

        for (area, area_name) in areas:
            png_basename = 'render-{}.png'.format(area_name)
            png_filename = join(dirname, png_basename)
            render.render(dirname, good_sources, 960, 2, png_filename, area)

            with open(png_filename, 'rb') as file:
                render_path = 'render-{}.png'.format(area_name)
                render_key = s3.new_key(join('/sets', str(the_set.id), png_basename))
                render_key.set_contents_from_string(file.read(), **key_kwargs)
    
            urls[area_name] = render_key.generate_url(**url_kwargs)

        db.execute('''UPDATE sets
                      SET render_world = %s, render_usa = %s, render_europe = %s
                      WHERE id = %s''',
                   (urls['world'], urls['usa'], urls['europe'], the_set.id))
    finally:
        rmtree(dirname)

def _prepare_render_sources(db, the_set, dirname):
    ''' Dump all non-null set runs into a directory for rendering.
    '''
    db.execute('''SELECT source_id, source_data, status FROM runs
                  WHERE set_id = %s AND status IS NOT NULL''',
               (the_set.id, ))
    
    good_sources = set()
    
    for (source_id, source_data, status) in db.fetchall():
        filename = '{source_id}.json'.format(**locals())
        with open(join(dirname, filename), 'w+b') as file:
            content = b64decode(source_data)
            file.write(content)
        
        if status is True:
            good_sources.add(filename)
    
    return good_sources

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

def create_queued_job(queue, files, job_url_template, commit_sha, status_url):
    ''' Create a new job, and add its files to the queue.
    '''
    filenames = list(files.keys())
    file_states = {name: None for name in filenames}
    file_results = {name: None for name in filenames}

    job_id = calculate_job_id(files)
    job_url = job_url_template and expand(job_url_template, dict(id=job_id))
    job_status = None

    with queue as db:
        task_files = add_files_to_queue(queue, job_id, job_url, files, commit_sha)
        add_job(db, job_id, None, task_files, file_states, file_results, status_url)
    
    return job_id

def add_files_to_queue(queue, job_id, job_url, files, commit_sha):
    ''' Make a new task for each file, return dict of file IDs to file names.
    '''
    tasks = {}
    
    for (file_name, (content_b64, file_id)) in files.items():
        task_data = dict(job_id=job_id, url=job_url, name=file_name,
                         content_b64=content_b64, file_id=file_id,
                         commit_sha=commit_sha)
    
        # Spread tasks out over time.
        delay = timedelta(seconds=len(tasks))

        queue.put(task_data, expected_at=td2str(delay))
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

def is_completed_run(db, run_id, min_datetime):
    '''
    '''
    if min_datetime.tzinfo:
        # Convert known time zones to UTC.
        min_dtz = min_datetime.astimezone(tzutc())
    else:
        # Assume unspecified time zones are UTC.
        min_dtz = min_datetime.replace(tzinfo=tzutc())

    db.execute('''SELECT id, status FROM runs
                  WHERE id = %s AND status IS NOT NULL
                    AND datetime_tz >= %s''',
               (run_id, min_dtz))
    
    completed_run = db.fetchone()
    
    if completed_run:
        _L.debug('Found completed run {0} ({1}) since {min_datetime}'.format(*completed_run, **locals()))
    else:
        _L.debug('No completed run {run_id} since {min_datetime}'.format(**locals()))
    
    return bool(completed_run is not None)

def add_run(db):
    ''' Reserve a row in the runs table and return its new ID.
    '''
    db.execute("INSERT INTO runs (datetime_tz) VALUES (NOW())")
    db.execute("SELECT currval('ints')")
    
    (run_id, ) = db.fetchone()
    
    return run_id

def set_run(db, run_id, filename, file_id, content_b64, run_state, run_status,
            job_id, worker_id, commit_sha, set_id):
    ''' Populate an identitified row in the runs table.
    '''
    db.execute('''UPDATE runs SET
                  source_path = %s, source_data = %s, source_id = %s,
                  state = %s::json, status = %s, worker_id = %s,
                  code_version = %s, job_id = %s, commit_sha = %s,
                  set_id = %s, datetime_tz = NOW()
                  WHERE id = %s''',
               (filename, content_b64, file_id,
               json.dumps(run_state), run_status, worker_id,
               __version__, job_id, commit_sha,
               set_id, run_id))

def copy_run(db, run_id, job_id, commit_sha, set_id):
    ''' Duplicate a previous run and return its new ID.
    
        Use new values for job ID, commit SHA, and set ID.
    '''
    db.execute('''INSERT INTO runs
                  (copy_of, source_path, source_id, source_data, state, status,
                   worker_id, code_version, job_id, commit_sha, set_id, datetime_tz)
                  SELECT id, source_path, source_id, source_data, state, status,
                         worker_id, code_version, %s, %s, %s, NOW()
                  FROM runs
                  WHERE id = %s''',
               (job_id, commit_sha, set_id, run_id))

    db.execute("SELECT currval('ints')")
    
    (run_id, ) = db.fetchone()
    
    return run_id

def get_previously_completed_run(db, file_id):
    ''' Look for an existing run on this file ID within the reuse timeout limit.
    '''
    interval = '{} seconds'.format(RUN_REUSE_TIMEOUT.seconds + RUN_REUSE_TIMEOUT.days * 86400)
    
    db.execute('''SELECT id, state, status FROM runs
                  WHERE source_id = %s
                    AND datetime_tz > NOW() - INTERVAL %s
                    AND status IS NOT NULL
                    AND copy_of IS NULL
                  ORDER BY id DESC LIMIT 1''',
               (file_id, interval))
    
    previous_run = db.fetchone()
    
    if previous_run:
        _L.debug('Found previous run {0} ({2}) for file {file_id}'.format(*previous_run, **locals()))
    else:
        _L.debug('No previous run for file {file_id}'.format(**locals()))

    return previous_run

def update_job_status(db, job_id, job_url, filename, run_status, results, github_auth):
    '''
    '''
    try:
        _, task_files, file_states, file_results, status_url = read_job(db, job_id)
    except TypeError:
        raise Exception('Job {} not found'.format(job_id))

    if filename not in file_states:
        raise Exception('Unknown file from job {}: "{}"'.format(job_id, filename))
    
    filenames = list(task_files.values())
    file_states[filename] = run_status
    file_results[filename] = results
    
    # Update job status.

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

def pop_task_from_taskqueue(s3, task_queue, done_queue, due_queue, output_dir):
    '''
    '''
    with task_queue as db:
        task = task_queue.get()

        # PQ will return NULL after 1 second timeout if not ask
        if task is None:
            return

        _L.info(u'Got file {name} from task queue'.format(**task.data))
        passed_on_keys = 'job_id', 'file_id', 'name', 'url', 'content_b64', 'commit_sha', 'set_id'
        passed_on_kwargs = {k: task.data.get(k) for k in passed_on_keys}
        passed_on_kwargs['worker_id'] = hex(getnode()).rstrip('L')

        previous_run = get_previously_completed_run(db, task.data.get('file_id'))
    
        if previous_run:
            # Make a copy of the previous run.
            previous_run_id, _, _ = previous_run
            copy_args = (passed_on_kwargs[k] for k in ('job_id', 'commit_sha', 'set_id'))
            passed_on_kwargs['run_id'] = copy_run(db, previous_run_id, *copy_args)
            
            # Don't send a due task, since we will not be doing any actual work.
        
        else:
            # Reserve space for a new run.
            passed_on_kwargs['run_id'] = add_run(db)

            # Send a Due task, possibly for later.
            due_task_data = dict(task_data=task.data, **passed_on_kwargs)
            due_queue.put(due_task_data, schedule_at=td2str(jobs.JOB_TIMEOUT + DUETASK_DELAY))
    
    if previous_run:
        # Re-use result from the previous run.
        run_id, state, status = previous_run
        message = MAGIC_OK_MESSAGE if status else 'Re-using failed previous run'
        result = dict(message=message, reused_run=run_id, output=state)

    else:
        # Run the task.
        from . import worker # <-- TODO: un-suck this.
        source_name, _ = splitext(relpath(passed_on_kwargs['name'], 'sources'))
        result = worker.do_work(s3, passed_on_kwargs['run_id'], source_name,
                                passed_on_kwargs['content_b64'], output_dir)

    # Send a Done task
    done_task_data = dict(result=result, **passed_on_kwargs)
    done_queue.put(done_task_data, expected_at=td2str(timedelta(0)))
    _L.info('Done')
    
    # Sleep a short time to allow done task to show up in runs table.
    # In a one-worker situation with repetitive pull request jobs,
    # this helps the next job take advantage of previous run results.
    sleep(WORKER_COOLDOWN.seconds + WORKER_COOLDOWN.days * 86400)

def pop_task_from_donequeue(queue, github_auth):
    ''' Look for a completed job in the "done" task queue, update Github status.
    '''
    with queue as db:
        task = queue.get()
    
        if task is None:
            return
    
        _L.info(u'Got file {name} from done queue'.format(**task.data))
        results = task.data['result']
        message = results['message']
        run_state = results.get('output', None)
        content_b64 = task.data['content_b64']
        commit_sha = task.data['commit_sha']
        worker_id = task.data.get('worker_id')
        set_id = task.data.get('set_id')
        job_url = task.data['url']
        filename = task.data['name']
        file_id = task.data['file_id']
        run_id = task.data['run_id']
        job_id = task.data['job_id']
        
        if is_completed_run(db, run_id, task.enqueued_at):
            # We are too late, this got handled.
            return
        
        run_status = bool(message == MAGIC_OK_MESSAGE)
        set_run(db, run_id, filename, file_id, content_b64, run_state,
                run_status, job_id, worker_id, commit_sha, set_id)

        if job_id:
            update_job_status(db, job_id, job_url, filename, run_status, results, github_auth)

def pop_task_from_duequeue(queue, github_auth):
    '''
    '''
    with queue as db:
        task = queue.get()
    
        if task is None:
            return
        
        _L.info(u'Got file {name} from due queue'.format(**task.data))
        original_task = task.data['task_data']
        content_b64 = task.data['content_b64']
        commit_sha = task.data['commit_sha']
        worker_id = task.data.get('worker_id')
        set_id = task.data.get('set_id')
        job_url = task.data['url']
        filename = task.data['name']
        file_id = task.data['file_id']
        run_id = task.data['run_id']
        job_id = task.data['job_id']
    
        if is_completed_run(db, run_id, task.enqueued_at):
            # Everything's fine, this got handled.
            return

        run_status = False
        set_run(db, run_id, filename, file_id, content_b64, None, run_status,
                job_id, worker_id, commit_sha, set_id)

        if job_id:
            update_job_status(db, job_id, job_url, filename, run_status, False, github_auth)

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
