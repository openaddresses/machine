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
    
    @property
    def length(self):
        return len(self.task_files.keys())
    
class Set:
    '''
    '''
    def __init__(self, id, commit_sha, datetime_start, datetime_end):
        '''
        '''
        self.id = id
        self.commit_sha = commit_sha
        self.datetime_start = datetime_start
        self.datetime_end = datetime_end
    
def read_jobs(db, past_id):
    ''' Read information about recent jobs.
    
        Returns list of Jobs.
    '''
    db.execute('''SELECT id, status, task_files, file_states, file_results, github_status_url
                  FROM jobs WHERE id > %s
                  ORDER BY id LIMIT 25''',
               (past_id, ))
    
    return [Job(*row) for row in db.fetchall()]

def read_set(db, set_id):
    '''
    '''
    db.execute('''SELECT id, commit_sha, datetime_start, datetime_end
                  FROM sets WHERE id = %s''', (set_id, ))
    
    try:
        id, commit_sha, datetime_start, datetime_end = db.fetchone()
    except TypeError:
        return None
    else:
        return Set(id, commit_sha, datetime_start, datetime_end)
    
def read_sets(db, past_id):
    ''' Read information about recent sets.
    
        Returns list of Sets.
    '''
    db.execute('''SELECT id, commit_sha, datetime_start, datetime_end
                  FROM sets WHERE id < COALESCE(%s, 2^32)
                  ORDER BY id DESC LIMIT 25''',
               (past_id, ))
    
    return [Set(*row) for row in db.fetchall()]
