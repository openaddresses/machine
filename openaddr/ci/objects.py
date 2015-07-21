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
    
def read_jobs(db, after_id):
    ''' Read information about recent jobs.
    
        Returns Job or None.
    '''
    db.execute('''SELECT id, status, task_files, file_states, file_results, github_status_url
                  FROM jobs WHERE id > %s
                  ORDER BY id LIMIT 25''',
               (after_id, ))
    
    return [Job(*row) for row in db.fetchall()]
