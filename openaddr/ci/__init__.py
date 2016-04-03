import logging; _L = logging.getLogger('openaddr.ci')

from ..compat import standard_library, expand_uri
from .. import jobs, render

from .objects import (
    add_job, write_job, read_job, complete_set, update_set_renders,
    add_run, set_run, copy_run, read_completed_set_runs,
    get_completed_file_run, get_completed_run, new_read_completed_set_runs
    )

from . import objects

from os.path import relpath, splitext, join, basename
from datetime import timedelta
from uuid import uuid4, getnode
from base64 import b64decode
from tempfile import mkdtemp
from functools import wraps
from shutil import rmtree
from time import time, sleep
import threading, sys
import json, os

from flask import Flask, request, Response, current_app, jsonify, render_template
from requests import get, post, ConnectionError
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
                MEMCACHE_SERVER=os.environ.get('MEMCACHE_SERVER'),
                DATABASE_URL=os.environ['DATABASE_URL'],
                WEBHOOK_SECRETS=webhook_secrets)

MAGIC_OK_MESSAGE = 'Everything is fine'
TASK_QUEUE, DONE_QUEUE, DUE_QUEUE, HEARTBEAT_QUEUE = 'tasks', 'finished', 'due', 'heartbeat'

# Additional delay after JOB_TIMEOUT for due tasks.
DUETASK_DELAY = timedelta(minutes=5)

# Amount of time to reuse run results.
RUN_REUSE_TIMEOUT = timedelta(days=5)

# Time to chill out in pop_task_from_taskqueue() after sending Done task.
WORKER_COOLDOWN = timedelta(seconds=5)

# Time to chill out in find_batch_sources() after failing a request.
GITHUB_RETRY_DELAY = timedelta(seconds=5)

# Time to wait between heartbeat pings from workers.
HEARTBEAT_INTERVAL = timedelta(minutes=5)

# Valid worker kinds for heartbeats table.
PERMANENT_KIND, TEMPORARY_KIND = 'permanent', 'temporary'

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
    compare1_url = expand_uri(compare1_url, dict(base='master', head=branch_sha))
    current_app.logger.debug('Compare URL 1 {}'.format(compare1_url))
    
    compare1 = get(compare1_url, auth=github_auth).json()
    merge_base_sha = compare1['merge_base_commit']['sha']
    
    # That's no branch.
    if merge_base_sha == branch_sha:
        return set()

    compare2_url = payload['repository']['compare_url']
    compare2_url = expand_uri(compare2_url, dict(base=merge_base_sha, head=branch_sha))
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
    compare_url = expand_uri(compare_url, dict(head=head_sha, base=base_sha))
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
        contents_url = expand_uri(contents_url, dict(path=filename, ref=commit_sha))
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
        contents_url = expand_uri(contents_url, dict(path=filename, ref=commit_sha))
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

def get_commit_info(app, payload):
    ''' Get owner, repository, commit SHA and Github status API URL from webhook payload.
    '''
    if 'pull_request' in payload:
        commit_sha = payload['pull_request']['head']['sha']
        status_url = payload['pull_request']['statuses_url']
    
    elif 'head_commit' in payload:
        commit_sha = payload['head_commit']['id']
        status_url = payload['repository']['statuses_url']
        status_url = expand_uri(status_url, dict(sha=commit_sha))
    
    else:
        raise ValueError('Unintelligible payload')
    
    if 'repository' not in payload:
        raise ValueError('Unintelligible payload')

    repo = payload['repository']
    owner = repo['owner'].get('name') or repo['owner'].get('login')
    repository = repo['name']
    
    app.logger.debug('Status URL {}'.format(status_url))
    
    return owner, repository, commit_sha, status_url

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

def find_batch_sources(owner, repository, github_auth, run_times={}):
    ''' Starting with a Github repo API URL, generate a stream of master sources.
    '''
    source_urls = list(_find_batch_source_urls(owner, repository, github_auth))
    
    # sort with the shortest known runs at the end, keeping the 9999's alphabetical.
    source_urls.sort(key=lambda su: su['path'])
    source_urls.sort(key=lambda su: (run_times.get(su['path']) or '9999'), reverse=True)
    
    for source_url in source_urls:
        _L.debug('Getting source {url}'.format(**source_url))
        try:
            more_source = get(source_url['url'], auth=github_auth).json()
        except ConnectionError:
            _L.info('Retrying to download {url}'.format(**source))
            try:
                sleep(GITHUB_RETRY_DELAY.seconds + GITHUB_RETRY_DELAY.days * 86400)
                more_source = get(source_url['url'], auth=github_auth).json()
            except ConnectionError:
                _L.error('Failed to download {url}'.format(**source_url))
                raise

        source = dict(content=more_source['content'])
        source.update(source_url)
        
        yield source

def get_batch_run_times(db, owner, repository):
    ''' Return dictionary of source paths to run time strings like '00:01:23'.
    '''
    last_set = objects.read_latest_set(db, owner, repository)

    return {run.source_path: run.state['process time']
            for run in objects.read_completed_runs_to_date(db, last_set.id)
            if run.state}

def _find_batch_source_urls(owner, repository, github_auth):
    ''' Starting with a Github repo API URL, return a list of sources.
    
        Sources are dictionaries, with keys commit_sha, url to content on Github,
        blob_sha for git blob, and path like 'sources/xx/yy.json'.
    '''
    resp = get('https://api.github.com/', auth=github_auth)
    if resp.status_code >= 400:
        raise Exception('Got status {} from Github API'.format(resp.status_code))
    start_url = expand_uri(resp.json()['repository_url'], dict(owner=owner, repo=repository))
    
    _L.info('Starting batch sources at {start_url}'.format(**locals()))
    got = get(start_url, auth=github_auth).json()
    contents_url, commits_url = got['contents_url'], got['commits_url']

    master_url = expand_uri(commits_url, dict(sha=got['default_branch']))

    _L.debug('Getting {ref} branch {master_url}'.format(ref=got['default_branch'], **locals()))
    got = get(master_url, auth=github_auth).json()
    commit_sha, commit_date = got['sha'], got['commit']['committer']['date']
    
    contents_url += '{?ref}' # So that we are consistently at the same commit.
    sources_urls = [expand_uri(contents_url, dict(path='sources', ref=commit_sha))]
    sources_list = list()

    for sources_url in sources_urls:
        _L.debug('Getting sources {sources_url}'.format(**locals()))
        sources = get(sources_url, auth=github_auth).json()
    
        for source in sources:
            if source['type'] == 'dir':
                params = dict(path=source['path'], ref=commit_sha)
                sources_urls.append(expand_uri(contents_url, params))
                continue
        
            if source['type'] != 'file':
                continue
        
            path_base, ext = splitext(source['path'])
        
            if ext == '.json':
                sources_list.append(dict(commit_sha=commit_sha, url=source['url'],
                                         blob_sha=source['sha'], path=source['path']))

    return sources_list

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
        complete_set(db, the_set.id, commit_sha)

    yield 0

def _update_expected_paths(db, expected_paths, the_set):
    ''' Discard sources from expected_paths set as they appear in runs table.
    '''
    for (_, source_path, _, _) in read_completed_set_runs(db, the_set.id):
        _L.debug(u'Discarding {}'.format(source_path))
        expected_paths.discard(source_path)

def render_index_maps(s3, runs):
    ''' Render index maps and upload them to S3.
    '''
    dirname = mkdtemp(prefix='index-maps-')

    try:
        good_runs = [run for run in runs if (run.state or {}).get('processed')]
        good_sources = _prepare_render_sources(good_runs, dirname)
        _render_and_upload_maps(s3, good_sources, '/', dirname)
    finally:
        rmtree(dirname)

def render_set_maps(s3, db, the_set):
    ''' Render set maps, upload them to S3 and add to the database.
    '''
    dirname = mkdtemp(prefix='set-maps-')

    try:
        s3_prefix = join('/sets', str(the_set.id))
        runs = new_read_completed_set_runs(db, the_set.id)
        good_sources = _prepare_render_sources(runs, dirname)
        s3_urls = _render_and_upload_maps(s3, good_sources, s3_prefix, dirname)
        update_set_renders(db, the_set.id, *s3_urls)
    finally:
        rmtree(dirname)

def _render_and_upload_maps(s3, good_sources, s3_prefix, dirname):
    ''' Render set maps, upload them to S3 and return their URLs.
    '''
    urls = dict()
    areas = (render.WORLD, 'world'), (render.USA, 'usa'), (render.EUROPE, 'europe')
    
    # Safe to force_http=False because we set boto.s3.connection.OrdinaryCallingFormat
    url_kwargs = dict(expires_in=0, query_auth=False, force_http=False)
    key_kwargs = dict(policy='public-read', headers={'Content-Type': 'image/png'})

    for (area, area_name) in areas:
        png_basename = 'render-{}.png'.format(area_name)
        png_filename = join(dirname, png_basename)
        render.render(dirname, good_sources, 960, 2, png_filename, area)

        with open(png_filename, 'rb') as file:
            render_path = 'render-{}.png'.format(area_name)
            render_key = s3.new_key(join(s3_prefix, png_basename))
            render_key.set_contents_from_string(file.read(), **key_kwargs)

        urls[area_name] = render_key.generate_url(**url_kwargs)
    
    return urls['world'], urls['usa'], urls['europe']

def _prepare_render_sources(runs, dirname):
    ''' Dump all non-null set runs into a directory for rendering.
    '''
    good_sources = set()
    
    for run in runs:
        filename = '{source_id}.json'.format(**run.__dict__)
        with open(join(dirname, filename), 'w+b') as file:
            content = b64decode(run.source_data)
            file.write(content)
        
        if run.status is True:
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

def create_queued_job(queue, files, job_url_template, commit_sha, owner, repo, status_url):
    ''' Create a new job, and add its files to the queue.
    '''
    filenames = list(files.keys())
    file_states = {name: None for name in filenames}
    file_results = {name: None for name in filenames}

    job_id = calculate_job_id(files)
    job_url = job_url_template and expand_uri(job_url_template, dict(id=job_id))
    job_status = None

    with queue as db:
        task_files = add_files_to_queue(queue, job_id, job_url, files, commit_sha)
        add_job(db, job_id, None, task_files, file_states, file_results, owner, repo, status_url)
    
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

def is_completed_run(db, run_id, min_datetime):
    '''
    '''
    if min_datetime.tzinfo:
        # Convert known time zones to UTC.
        min_dtz = min_datetime.astimezone(tzutc())
    else:
        # Assume unspecified time zones are UTC.
        min_dtz = min_datetime.replace(tzinfo=tzutc())

    completed_run = get_completed_run(db, run_id, min_dtz)
    
    if completed_run:
        _L.debug('Found completed run {0} ({1}) since {min_datetime}'.format(*completed_run, **locals()))
    else:
        _L.debug('No completed run {run_id} since {min_datetime}'.format(**locals()))
    
    return bool(completed_run is not None)

def update_job_status(db, job_id, job_url, filename, run_status, results, github_auth):
    '''
    '''
    try:
        job = read_job(db, job_id)
    except TypeError:
        raise Exception('Job {} not found'.format(job_id))

    if filename not in job.states:
        raise Exception('Unknown file from job {}: "{}"'.format(job.id, filename))
    
    filenames = list(job.task_files.values())
    job.states[filename] = run_status
    job.file_results[filename] = results
    
    # Update job status.

    if False in job.states.values():
        # Any task failure means the whole job has failed.
        job.status = False
    elif None in job.states.values():
        job.status = None
    else:
        job.status = True
    
    write_job(db, job.id, job.status, job.task_files, job.states, job.file_results,
              job.github_owner, job.github_repository, job.github_status_url)
    
    if not job.github_status_url:
        _L.warning('No status_url to tell about {} status of job {}'.format(job.status, job.id))
        return
    
    if job.status is False:
        bad_files = [name for (name, state) in job.states.items() if state is False]
        update_failing_status(job.github_status_url, job_url, bad_files, filenames, github_auth)
    
    elif job.status is None:
        update_pending_status(job.github_status_url, job_url, filenames, github_auth)
    
    elif job.status is True:
        update_success_status(job.github_status_url, job_url, filenames, github_auth)

def is_merged_to_master(db, set_id, job_id, commit_sha, github_auth):
    '''
    '''
    # use objects.read_set and read_job so they can be mocked in testing.
    set, job = objects.read_set(db, set_id), objects.read_job(db, job_id)
    
    if set is not None:
        # Sets come from master by definition.
        return True
    
    elif job is None:
        # Missing set and job means unknown merge status.
        return None
    
    try:
        template1 = get('https://api.github.com/', auth=github_auth).json().get('repository_url')
        repo_url = expand_uri(template1, dict(owner=job.github_owner, repo=job.github_repository))
    
        template2 = get(repo_url, auth=github_auth).json().get('compare_url')
        compare_url = expand_uri(template2, dict(base=commit_sha, head='master'))
    
        compare = get(compare_url, auth=github_auth).json()
        return compare['base_commit']['sha'] == compare['merge_base_commit']['sha']

    except Exception as e:
        _L.error('Failed to check merged status of {}/{} {}: {}'\
            .format(job.github_owner, job.github_repository, commit_sha, e))
        return None

def _worker_id():
    return hex(getnode()).rstrip('L')

def _wait_for_work_lock(lock, heartbeat_queue, worker_kind):
    ''' Wait around for worker while sending heartbeat pings.
    '''
    next_put = time()

    while True:
        sleep(.1)

        if lock.acquire(False):
            # Got the lock, we are done.
            break
        
        if time() > next_put:
            # Keep this put() outside the lock, so threads don't confuse Postgres.
            heartbeat_queue.put({'worker_id': _worker_id(), 'worker_kind': worker_kind})
            next_put += HEARTBEAT_INTERVAL.seconds + HEARTBEAT_INTERVAL.days * 86400

def pop_task_from_taskqueue(s3, task_queue, done_queue, due_queue, heartbeat_queue, output_dir, worker_kind):
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
        passed_on_kwargs['worker_id'] = _worker_id()

        interval = '{} seconds'.format(RUN_REUSE_TIMEOUT.seconds + RUN_REUSE_TIMEOUT.days * 86400)
        previous_run = get_completed_file_run(db, task.data.get('file_id'), interval)
    
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

        work_lock = threading.Lock()
        work_args = work_lock, heartbeat_queue, worker_kind
        work_wait = threading.Thread(target=_wait_for_work_lock, args=work_args)

        with work_lock:
            heartbeat_queue.put({'worker_id': _worker_id(), 'worker_kind': worker_kind})
            work_wait.start()

            source_name, _ = splitext(relpath(passed_on_kwargs['name'], 'sources'))
            result = worker.do_work(s3, passed_on_kwargs['run_id'], source_name,
                                    passed_on_kwargs['content_b64'], output_dir)
        
        work_wait.join()

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
        is_merged = is_merged_to_master(db, set_id, job_id, commit_sha, github_auth)
        
        set_run(db, run_id, filename, file_id, content_b64, run_state,
                run_status, job_id, worker_id, commit_sha, is_merged, set_id)

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
        is_merged = is_merged_to_master(db, set_id, job_id, commit_sha, github_auth)

        set_run(db, run_id, filename, file_id, content_b64, None, run_status,
                job_id, worker_id, commit_sha, is_merged, set_id)

        if job_id:
            update_job_status(db, job_id, job_url, filename, run_status, False, github_auth)

def flush_heartbeat_queue(queue):
    ''' Clear out heartbeat queue, logging each one.
    '''
    with queue as db:
        for task in queue:
            if task is None:
                break

            _L.info('Got heartbeat {}: {}'.format(task.id, task.data))
            id, kind = task.data.get('worker_id'), task.data.get('worker_kind')
            
            db.execute('''DELETE FROM heartbeats
                          WHERE worker_id = %s
                            AND ((worker_kind IS NULL AND %s IS NULL)
                                 OR worker_kind = %s)''',
                       (id, kind, kind))
            
            db.execute('''INSERT INTO heartbeats (worker_id, worker_kind, datetime)
                          VALUES (%s, %s, NOW())''',
                       (id, kind))

def get_recent_workers(db):
    '''
    '''
    recent_workers = {PERMANENT_KIND: [], TEMPORARY_KIND: [], None: []}
    
    db.execute('''SELECT worker_kind, worker_id
                  FROM heartbeats WHERE datetime >= NOW() - INTERVAL %s''',
               (HEARTBEAT_INTERVAL * 3, ))

    for (kind, id) in db.fetchall():
        recent_workers[kind].append(id)
    
    return recent_workers

def db_connect(dsn=None, user=None, password=None, host=None, port=None, database=None, sslmode=None):
    ''' Connect to database.
    
        Use DSN string if given, but allow other calls for older systems.
    '''
    if dsn is None:
        return connect(user=user, password=password, host=host, port=port, database=database, sslmode=sslmode)

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
        
        if hasattr(record, 'request_info'):
            subject = '{} - {}'.format(subject, record.request_info)
        
        self.sns.publish(self.arn, self.format(record), subject[:79])

# Remember whether logger has been set up before.
_logger_status = {'set up': False}

def setup_logger(sns_arn, log_level=logging.DEBUG):
    ''' Set up logging for openaddr code.
    '''
    if _logger_status['set up']:
        # Do this exactly once, so repeated handlers don't stack up.
        return
    else:
        _logger_status['set up'] = True
    
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

def log_function_errors(route_function):
    ''' Error-logging decorator for functions.
    
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

if __name__ == '__main__':
    app.run(debug=True)
