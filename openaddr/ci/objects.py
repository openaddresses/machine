import logging; _L = logging.getLogger('openaddr.ci.objects')

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
                 render_world, render_europe, render_usa):
        '''
        '''
        self.id = id
        self.commit_sha = commit_sha
        self.datetime_start = datetime_start
        self.datetime_end = datetime_end

        self.render_world = render_world
        self.render_europe = render_europe
        self.render_usa = render_usa
    
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
                         render_world, render_europe, render_usa
                  FROM sets WHERE id = %s''', (set_id, ))
    
    try:
        id, c_sha, dt_start, dt_end, r_world, r_europe, r_usa = db.fetchone()
    except TypeError:
        return None
    else:
        return Set(id, c_sha, dt_start, dt_end, r_world, r_europe, r_usa)
    
def read_sets(db, past_id):
    ''' Read information about recent sets.
    
        Returns list of Sets.
    '''
    db.execute('''SELECT id, commit_sha, datetime_start, datetime_end
                  FROM sets WHERE id < COALESCE(%s, 2^64)
                  ORDER BY id DESC LIMIT 25''',
               (past_id, ))
    
    return [Set(*row) for row in db.fetchall()]
