from os import environ
from os.path import splitext, relpath
from requests import get
from uritemplate import expand
from time import sleep

from . import add_run, set_run, db_connect, db_queue, TASK_QUEUE

auth = environ['GITHUB_TOKEN'], 'x-oauth-basic'
start_url = 'https://api.github.com/repos/openaddresses/openaddresses'
print('Starting at {start_url}'.format(**locals()))
got = get(start_url, auth=auth).json()
contents_url, commits_url = got['contents_url'], got['commits_url']

master_url = expand(commits_url, dict(sha=got['default_branch']))

print('Getting {ref} branch {master_url}'.format(ref=got['default_branch'], **locals()))
got = get(master_url, auth=auth).json()
commit_sha, commit_date = got['sha'], got['commit']['committer']['date']

contents_url += '{?ref}' # So that we are consistently at the same commit.
sources_urls = [expand(contents_url, dict(path='sources', ref=commit_sha))]
sources_dict = dict()

for sources_url in sources_urls:
    print('Getting sources {sources_url}'.format(**locals()))
    sources = get(sources_url, auth=auth).json()
    
    for source in sources:
        if source['type'] == 'dir':
            params = dict(path=source['path'], ref=commit_sha)
            sources_urls.append(expand(contents_url, params))
            continue
        
        if source['type'] != 'file':
            continue
        
        path_base, ext = splitext(source['path'])
        
        if ext == '.json':
            key = relpath(path_base, 'sources')
            val = dict(url=source['url'], sha=source['sha'], path=source['path'])
            sources_dict[key] = val
            continue
            
            source = get(source['url'], auth=auth).json()
            path = relpath(source['path'], 'sources')
            bytes = len(source['content'])
            
            print('{} bytes of {encoding}-encoded data in {}'.format(bytes, path, **source))

print(sources_dict)

with db_connect(environ['DATABASE_URL']) as conn:
    queue = db_queue(conn, TASK_QUEUE)
    for source in sources_dict.values():
        while len(queue) >= 1:
            print('sleeping because queue is', len(queue), 'long')
            sleep(3)
            
        with queue as db:
            more_source = get(source['url'], auth=auth).json()

            task_data = dict(job_id=None, url=None, name=source['path'],
                             content_b64=more_source['content'],
                             file_id=source['sha'],
                             commit_sha=commit_sha)
            
            task_id = queue.put(task_data)
            print('enqueued task', task_id)
        
        #run_id = add_run(db)
        #set_run(db, run_id, filename, file_id, content_b64, run_state, run_status,
        #    job_id, worker_id, commit_sha)
