import logging; _L = logging.getLogger('openaddr.ci.objects')

from .. import __version__
import json, pickle

# Todo: make this a Python 3 enum
FAIL_REASONS = {
    None, 'Source says to skip', 'Source is missing a conform object',
    'Unknown source conform type', 'Could not download source data',
    'Could not conform source data'
    }

class Job:
    '''
    '''
    def __init__(self, id, status, task_files, states, file_results,
                 github_owner, github_repository, github_status_url,
                 github_comments_url, datetime_start, datetime_end):
        '''
        '''
        self.id = id
        self.status = status
        self.task_files = task_files
        self.states = states
        self.file_results = file_results
        self.github_owner = github_owner
        self.github_repository = github_repository
        self.github_status_url = github_status_url
        self.github_comments_url = github_comments_url
        self.datetime_start = datetime_start
        self.datetime_end = datetime_end
    
class Set:
    '''
    '''
    def __init__(self, id, commit_sha, datetime_start, datetime_end,
                 render_world, render_europe, render_usa, owner, repository):
        '''
        '''
        self.id = id
        self.commit_sha = commit_sha
        self.datetime_start = datetime_start
        self.datetime_end = datetime_end

        self.render_world = render_world
        self.render_europe = render_europe
        self.render_usa = render_usa

        self.owner = owner
        self.repository = repository

class Run:
    '''
    '''
    def __init__(self, id, source_path, source_id, source_data, datetime_tz,
                 state, status, copy_of, code_version, worker_id, job_id,
                 set_id, commit_sha, is_merged):
        '''
        '''
        assert hasattr(state, 'to_json'), 'Run state should have to_json() method'
        
        self.id = id
        self.source_path = source_path
        self.source_id = source_id
        self.source_data = bytes(source_data) if (source_data is not None) else None
        self.datetime_tz = datetime_tz

        self.state = state
        self.status = status
        self.copy_of = copy_of

        self.code_version = code_version
        self.worker_id = worker_id
        self.job_id = job_id
        self.set_id = set_id
        self.commit_sha = commit_sha
        self.is_merged = is_merged

class RunState:
    '''
    '''
    # Dictionary of acceptable keys for the input json blob.
    key_attrs = {key: key.replace(' ', '_').replace('-', '_')
        for key in ('source', 'cache', 'sample', 'geometry type',
        'address count', 'version', 'fingerprint', 'cache time', 'processed',
        'output', 'process time', 'website', 'skipped', 'license',
        'share-alike', 'attribution required', 'attribution name',
        'attribution flag', 'process hash', 'preview', 'fail reason')}

    def __init__(self, json_blob):
        blob_dict = dict(json_blob or {})
        self.keys = blob_dict.keys()
        
        self.source = blob_dict.get('source')
        self.cache = blob_dict.get('cache')
        self.sample = blob_dict.get('sample')
        self.geometry_type = blob_dict.get('geometry type')
        self.address_count = blob_dict.get('address count')
        self.version = blob_dict.get('version')
        self.fingerprint = blob_dict.get('fingerprint')
        self.cache_time = blob_dict.get('cache time')
        self.processed = blob_dict.get('processed')
        self.output = blob_dict.get('output')
        self.preview = blob_dict.get('preview')
        self.process_time = blob_dict.get('process time')
        self.process_hash = blob_dict.get('process hash')
        self.website = blob_dict.get('website')
        self.skipped = blob_dict.get('skipped')
        self.license = blob_dict.get('license')
        self.share_alike = blob_dict.get('share-alike')
        self.attribution_required = blob_dict.get('attribution required')
        self.attribution_name = blob_dict.get('attribution name')
        self.attribution_flag = blob_dict.get('attribution flag')
        self.fail_reason = blob_dict.get('fail reason')

        unexpected = ', '.join(set(self.keys) - set(RunState.key_attrs.keys()))
        assert len(unexpected) == 0, 'RunState should not have keys {}'.format(unexpected)
        
        assert self.fail_reason in FAIL_REASONS, 'Uknown failure reason {}'.format(repr(self.fail_reason))
    
    def get(self, json_key):
        if json_key == 'code version':
            # account for ci.webapi.CSV_HEADER mismatch
            json_key = 'version'
    
        return getattr(self, RunState.key_attrs[json_key])
        
    def to_json(self):
        return json.dumps({k: self.get(k) for k in self.keys})

class Zip:
    '''
    '''
    def __init__(self, url, content_length):
        self.url = url
        self.content_length = content_length

def add_job(db, job_id, status, task_files, file_states, file_results, owner, repo, status_url, comments_url):
    ''' Save information about a job to the database.
    
        Throws an IntegrityError exception if the job ID exists.
    '''
    db.execute('''INSERT INTO jobs
                  (task_files, file_states, file_results, github_owner,
                   github_repository, github_status_url, github_comments_url,
                   status, id, datetime_start)
                  VALUES (%s::json, %s::json, %s::json, %s, %s, %s, %s, %s, %s, NOW())''',
               (json.dumps(task_files), json.dumps(file_states),
                json.dumps(file_results), owner, repo, status_url,
                comments_url, status, job_id))

def write_job(db, job_id, status, task_files, file_states, file_results, owner, repo, status_url, comments_url):
    ''' Save information about a job to the database.
    '''
    is_complete = bool(status is not None)
    
    db.execute('''UPDATE jobs
                  SET task_files=%s::json, file_states=%s::json,
                      file_results=%s::json, github_owner=%s, github_repository=%s,
                      github_status_url=%s, github_comments_url=%s, status=%s,
                      datetime_end=CASE WHEN %s THEN NOW() ELSE null END
                  WHERE id = %s''',
               (json.dumps(task_files), json.dumps(file_states),
                json.dumps(file_results), owner, repo, status_url, comments_url,
                status, is_complete, job_id))

def read_job(db, job_id):
    ''' Read information about a job from the database.
    
        Returns a Job or None.
    '''
    db.execute('''SELECT status, task_files, file_states, file_results,
                         github_owner, github_repository, github_status_url,
                         github_comments_url, datetime_start, datetime_end
                  FROM jobs WHERE id = %s
                  LIMIT 1''', (job_id, ))
    
    try:
        status, task_files, states, file_results, github_owner, github_repository, \
        github_status_url, github_comments_url, datetime_start, datetime_end = db.fetchone()
    except TypeError:
        return None
    else:
        return Job(job_id, status, task_files, states, file_results,
                   github_owner, github_repository, github_status_url,
                   github_comments_url, datetime_start, datetime_end)
    
def read_jobs(db, past_id):
    ''' Read information about recent jobs.
    
        Returns list of Jobs.
    '''
    db.execute('''SELECT id, status, task_files, file_states, file_results,
                         github_owner, github_repository, github_status_url,
                         github_comments_url, datetime_start, datetime_end
                  --
                  -- Select sequence value from jobs based on ID. Null sequence
                  -- values will be excluded by this comparison to an integer.
                  --
                  FROM jobs WHERE sequence < COALESCE((SELECT sequence FROM jobs WHERE id = %s), 2^64)
                  ORDER BY sequence DESC LIMIT 25''',
               (past_id, ))
    
    return [Job(*row) for row in db.fetchall()]

def add_set(db, owner, repository):
    '''
    '''
    db.execute('''INSERT INTO sets
                  (owner, repository, datetime_start)
                  VALUES (%s, %s, NOW())''',
               (owner, repository))

    db.execute("SELECT CURRVAL('ints')")
    (set_id, ) = db.fetchone()

    _L.info(u'Added set {} to sets table'.format(set_id))

    return read_set(db, set_id)

def complete_set(db, set_id, commit_sha):
    '''
    '''
    _L.info(u'Updating set {} in sets table'.format(set_id))

    db.execute('''UPDATE sets
                  SET datetime_end = NOW(), commit_sha = %s
                  WHERE id = %s''',
               (commit_sha, set_id))

def update_set_renders(db, set_id, render_world, render_usa, render_europe):
    '''
    '''
    db.execute('''UPDATE sets
                  SET render_world = %s, render_usa = %s, render_europe = %s
                  WHERE id = %s''',
               (render_world, render_usa, render_europe, set_id))

def read_set(db, set_id):
    '''
    '''
    db.execute('''SELECT id, commit_sha, datetime_start, datetime_end,
                         render_world, render_europe, render_usa,
                         owner, repository
                  FROM sets WHERE id = %s
                  LIMIT 1''', (set_id, ))
    
    try:
        id, sha, start, end, world, europe, usa, own, repo = db.fetchone()
    except TypeError:
        return None
    else:
        return Set(id, sha, start, end, world, europe, usa, own, repo)
    
def read_sets(db, past_id):
    ''' Read information about recent sets.
    
        Returns list of Sets.
    '''
    db.execute('''SELECT id, commit_sha, datetime_start, datetime_end,
                         render_world, render_europe, render_usa,
                         owner, repository
                  FROM sets WHERE id < COALESCE(%s, 2^64)
                  ORDER BY id DESC LIMIT 25''',
               (past_id, ))
    
    return [Set(*row) for row in db.fetchall()]

def read_latest_set(db, owner, repository):
    ''' Read latest completed set with given owner and repository.
    '''
    db.execute('''SELECT id, commit_sha, datetime_start, datetime_end,
                         render_world, render_europe, render_usa,
                         owner, repository
                  FROM sets
                  WHERE owner = %s AND repository = %s
                    AND datetime_end IS NOT NULL
                  ORDER BY datetime_start DESC
                  LIMIT 1''',
               (owner, repository, ))
    
    try:
        id, sha, start, end, world, europe, usa, own, repo = db.fetchone()
    except TypeError:
        return None
    else:
        return Set(id, sha, start, end, world, europe, usa, own, repo)

def add_run(db):
    ''' Reserve a row in the runs table and return its new ID.
    '''
    db.execute("INSERT INTO runs (datetime_tz) VALUES (NOW())")
    db.execute("SELECT currval('ints')")
    
    (run_id, ) = db.fetchone()
    
    return run_id

def set_run(db, run_id, filename, file_id, content_b64, run_state, run_status,
            job_id, worker_id, commit_sha, is_merged, set_id):
    ''' Populate an identitified row in the runs table.
    '''
    db.execute('''UPDATE runs SET
                  source_path = %s, source_data = %s, source_id = %s,
                  state = %s::json, status = %s, worker_id = %s,
                  code_version = %s, job_id = %s, commit_sha = %s,
                  is_merged = %s, set_id = %s, datetime_tz = NOW()
                  WHERE id = %s''',
               (filename, content_b64, file_id,
               run_state.to_json(), run_status, worker_id,
               __version__, job_id, commit_sha, is_merged,
               set_id, run_id))

def copy_run(db, run_id, job_id, commit_sha, set_id):
    ''' Duplicate a previous run and return its new ID.
    
        Use new values for job ID, commit SHA, and set ID.
    '''
    db.execute('''INSERT INTO runs
                  (copy_of, source_path, source_id, source_data, state, status,
                   worker_id, code_version, job_id, commit_sha, is_merged, set_id, datetime_tz)
                  SELECT id, source_path, source_id, source_data, state, status,
                         worker_id, code_version, %s, %s, NULL, %s, NOW()
                  FROM runs
                  WHERE id = %s''',
               (job_id, commit_sha, set_id, run_id))

    db.execute("SELECT currval('ints')")
    
    (run_id, ) = db.fetchone()
    
    return run_id

def read_run(db, run_id):
    '''
    '''
    db.execute('''SELECT id, source_path, source_id, source_data, datetime_tz,
                         state, status, copy_of, code_version, worker_id,
                         job_id, set_id, commit_sha, is_merged
                  FROM runs WHERE id = %s
                  LIMIT 1''', (run_id, ))
    
    try:
        (id, source_path, source_id, source_data, datetime_tz, state, status, copy_of,
         code_version, worker_id, job_id, set_id, commit_sha, is_merged) = db.fetchone()
    except TypeError:
        return None
    else:
        return Run(id, source_path, source_id, source_data, datetime_tz,
                   RunState(state), status, copy_of, code_version, worker_id,
                   job_id, set_id, commit_sha, is_merged)
    
def get_completed_file_run(db, file_id, interval):
    ''' Look for an existing run on this file ID within the reuse timeout limit.
    '''
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

def get_completed_run(db, run_id, min_dtz):
    '''
    '''
    db.execute('''SELECT id, status FROM runs
                  WHERE id = %s AND status IS NOT NULL
                    AND datetime_tz >= %s
                    LIMIT 1''',
               (run_id, min_dtz))
    
    return db.fetchone()

def read_completed_set_runs(db, set_id):
    '''
    '''
    db.execute('''SELECT source_id, source_path, source_data, status FROM runs
                  WHERE set_id = %s AND status IS NOT NULL''',
               (set_id, ))
    
    return list(db.fetchall())

def new_read_completed_set_runs(db, set_id):
    '''
    '''
    db.execute('''SELECT id, source_path, source_id, source_data, datetime_tz,
                         state, status, copy_of, code_version, worker_id,
                         job_id, set_id, commit_sha, is_merged FROM runs
                  WHERE set_id = %s AND status IS NOT NULL''',
               (set_id, ))
    
    return [Run(*row[:5]+(RunState(row[5]),)+row[6:]) for row in db.fetchall()]

def read_completed_source_runs(db, source_path):
    '''
    '''
    db.execute('''SELECT id, source_path, source_id, source_data, datetime_tz,
                         state, status, copy_of, code_version, worker_id,
                         job_id, set_id, commit_sha, is_merged FROM runs
                  WHERE source_path = %s AND status IS NOT NULL
                    AND (is_merged or is_merged is null)
                  ORDER BY id DESC''',
               (source_path, ))
    
    seen, runs = set(), list()
    
    for row in db.fetchall():
        run = Run(*row[:5] + (RunState(row[5]),) + row[6:])
        
        if run.copy_of not in seen and run.id not in seen:
            runs.append(run)
        
        seen.add(run.id)
        if run.copy_of is not None:
            seen.add(run.copy_of)
    
    return runs

def read_completed_runs_to_date(db, starting_set_id):
    ''' Get only successful runs.
    '''
    set = read_set(db, starting_set_id)
    
    if set is None or set.datetime_end is None:
        return None
    
    # Get IDs for latest successful source runs of any run in the requested set.
    db.execute('''SELECT MAX(id), source_path FROM runs
                  WHERE source_path IN (
                      -- Get all source paths for successful runs in this set.
                      SELECT source_path FROM runs
                      WHERE set_id = %s
                    )
                    -- Get only successful, merged runs.
                    AND status = true
                    AND (is_merged = true OR is_merged IS NULL)
                  GROUP BY source_path''',
               (set.id, ))
    
    run_path_ids = {path: run_id for (run_id, path) in db.fetchall()}
    
    # Get IDs for latest unsuccessful source runs of any run in the requested set.
    db.execute('''SELECT MAX(id), source_path FROM runs
                  WHERE source_path IN (
                      -- Get all source paths for failed runs in this set.
                      SELECT source_path FROM runs
                      WHERE set_id = %s
                    )
                    -- Get only unsuccessful, merged runs.
                    AND status = false
                    AND (is_merged = true OR is_merged IS NULL)
                  GROUP BY source_path''',
               (set.id, ))
    
    # Use unsuccessful runs if no successful ones exist.
    for (run_id, source_path) in db.fetchall():
        if source_path not in run_path_ids:
            run_path_ids[source_path] = run_id
    
    run_ids = tuple(sorted(run_path_ids.values()))
    
    if not run_ids:
        return []

    # Get Run instance for each of the returned run IDs.
    db.execute('''SELECT id, source_path, source_id, source_data, datetime_tz,
                         state, status, copy_of, code_version, worker_id,
                         job_id, set_id, commit_sha, is_merged
                  FROM runs
                  WHERE id IN %s''',
               (run_ids, ))
    
    return [Run(*row[:5]+(RunState(row[5]),)+row[6:]) for row in db.fetchall()]

def read_latest_run(db, source_path):
    '''
    '''
    # Get ID for latest successful source run matching path.
    db.execute('''SELECT MAX(id) FROM runs
                  WHERE source_path = %s
                    -- Get only successful, merged run.
                    AND status = true
                    AND (is_merged = true OR is_merged IS NULL)''',
               (source_path, ))
    
    (run_id, ) = db.fetchone()
    
    if run_id is not None:
        return read_run(db, run_id)

    # Get ID for latest unsuccessful source run matching path.
    db.execute('''SELECT MAX(id) FROM runs
                  WHERE source_path = %s
                    -- Get only unsuccessful, merged runs.
                    AND status = false
                    AND (is_merged = true OR is_merged IS NULL)''',
               (source_path, ))

    # Use unsuccessful run if no successful one exists.
    (run_id, ) = db.fetchone()
    
    if run_id is not None:
        return read_run(db, run_id)
    
def load_collection_zips_dict(db):
    '''
    '''
    db.execute('''SELECT collection, license_attr, url, content_length
                  FROM zips WHERE is_current''')
    
    return {(coll, attr): Zip(url, len)
            for (coll, attr, url, len) in db.fetchall()}
