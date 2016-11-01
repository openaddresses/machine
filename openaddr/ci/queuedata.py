class Task:

    def __init__(self, job_id, url, name, content_b64, commit_sha, file_id,
                 rerun=None, set_id=None):
        self.job_id, self.url, self.name = job_id, url, name
        self.content_b64, self.commit_sha = content_b64, commit_sha
        self.file_id, self.rerun, self.set_id = file_id, rerun, set_id
    
    def enqueue(self, queue, expected_at=None):
        data = dict(job_id=self.job_id, url=self.url, name=self.name,
                    content_b64=self.content_b64, file_id=self.file_id,
                    commit_sha=self.commit_sha)
        
        if self.rerun is not None: data.update(rerun=self.rerun)
        if self.set_id is not None: data.update(set_id=self.set_id)

        queue.put(data, expected_at=expected_at)

class Due:

    def __init__(self, job_id, url, name, content_b64, commit_sha, file_id,
                 rerun, set_id, worker_id, run_id, **junk):
        self.job_id, self.url, self.name = job_id, url, name
        self.content_b64, self.commit_sha = content_b64, commit_sha
        self.file_id, self.rerun, self.set_id = file_id, rerun, set_id
        self.worker_id, self.run_id = worker_id, run_id
    
    def enqueue(self, queue, schedule_at):
        data = dict(job_id=self.job_id, url=self.url, name=self.name,
                    content_b64=self.content_b64, file_id=self.file_id,
                    commit_sha=self.commit_sha, rerun=self.rerun,
                    set_id=self.set_id, worker_id=self.worker_id,
                    run_id=self.run_id)
    
        queue.put(data, schedule_at=schedule_at)

class Done:

    def __init__(self, job_id, url, name, content_b64, commit_sha, file_id,
                 run_id, result, rerun=None, set_id=None, worker_id=None,
                 **junk):
        self.job_id, self.url, self.name = job_id, url, name
        self.content_b64, self.commit_sha = content_b64, commit_sha
        self.file_id, self.rerun, self.set_id = file_id, rerun, set_id
        self.worker_id, self.run_id = worker_id, run_id
        self.result = result
    
    def enqueue(self, queue, expected_at):
        data = dict(job_id=self.job_id, url=self.url, name=self.name,
                    content_b64=self.content_b64, file_id=self.file_id,
                    commit_sha=self.commit_sha, run_id=self.run_id,
                    result=self.result)
        
        if self.rerun is not None: data.update(rerun=self.rerun)
        if self.worker_id is not None: data.update(worker_id=self.worker_id)
        if self.set_id is not None: data.update(set_id=self.set_id)

        queue.put(data, expected_at=expected_at)

class Heartbeat:

    def __init__(self, worker_id, worker_kind):
        self.worker_id, self.worker_kind = worker_id, worker_kind
    
    def enqueue(self, queue):
        queue.put(dict(worker_id=self.worker_id, worker_kind=self.worker_kind))
