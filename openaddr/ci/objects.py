import logging; _L = logging.getLogger('openaddr.ci.objects')

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
