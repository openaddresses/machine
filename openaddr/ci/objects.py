import logging; _L = logging.getLogger('openaddr.ci.objects')

from .. import __version__
import json

class Job:
    '''
    '''
    def __init__(self, id, status, task_files, states, file_results, github_status_url):
        '''
        '''
        self.id = id
        self.status = status
        self.task_files = task_files
        self.states = states
        self.file_results = file_results
        self.github_status_url = github_status_url
    
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
    
        Returns a Job or None.
    '''
    db.execute('''SELECT status, task_files, file_states, file_results, github_status_url
                  FROM jobs WHERE id = %s''', (job_id, ))
    
    try:
        status, task_files, states, file_results, github_status_url = db.fetchone()
    except TypeError:
        return None
    else:
        return Job(job_id, status, task_files, states, file_results, github_status_url)
    
def read_jobs(db, past_id):
    ''' Read information about recent jobs.
    
        Returns list of Jobs.
    '''
    db.execute('''SELECT id, status, task_files, file_states, file_results, github_status_url
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
                  FROM sets WHERE id = %s''', (set_id, ))
    
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
                    AND datetime_tz >= %s''',
               (run_id, min_dtz))
    
    return db.fetchone()

def read_completed_set_runs(db, set_id):
    '''
    '''
    db.execute('''SELECT source_id, source_path, source_data, status FROM runs
                  WHERE set_id = %s AND status IS NOT NULL''',
               (set_id, ))
    
    return list(db.fetchall())
