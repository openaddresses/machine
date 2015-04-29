from os.path import relpath, splitext
from base64 import b64decode
import json, os

from flask import Flask, request, Response, current_app
from uritemplate import expand
from requests import get, post

app = Flask(__name__)
app.config['GITHUB_AUTH'] = os.environ['GITHUB_TOKEN'], 'x-oauth-basic'

@app.route('/')
def index():
    return 'Yo.'

@app.route('/hook', methods=['GET', 'POST'])
def hook():
    github_auth = current_app.config['GITHUB_AUTH']
    webhook_payload = json.loads(request.data)
    files = process_payload(webhook_payload, github_auth)
    status_url = get_status_url(webhook_payload)
    
    if files:
        update_pending_status(status_url, files, github_auth)
        add_to_job_queue(files)
    else:
        update_empty_status(status_url, github_auth)
    
    response = '\n\n'.join(['{}:\n\n{}\n'.format(name, data)
                            for (name, data) in sorted(files.items())])
    
    return Response(response, headers={'Content-Type': 'text/plain'})

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
            touched.remove(filename)
        
    current_app.logger.debug('Touched files {}'.format(', '.join(touched)))
    
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
    current_app.logger.debug('Touched files {}'.format(', '.join(touched)))
    
    return touched

def process_payload(payload, github_auth):
    ''' Return a dictionary of file paths and raw JSON contents.
    '''
    processed = dict()

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
        
        contents_url = payload['repository']['contents_url']
        contents_url = expand(contents_url, dict(path=filename))
        contents_url = '{contents_url}?ref={commit_sha}'.format(**locals())
        
        current_app.logger.debug('Contents URL {}'.format(contents_url))
        
        got = get(contents_url, auth=github_auth)
        contents = got.json()
        
        if got.status_code not in range(200, 299):
            current_app.logger.warning('Skipping {} - {}'.format(filename, got.status_code))
            continue
        
        content, encoding = contents['content'], contents['encoding']
        
        current_app.logger.debug('Contents SHA {}'.format(contents['sha']))
        
        if encoding == 'base64':
            processed[filename] = b64decode(content)
        else:
            raise ValueError('Unrecognized encoding "{}"'.format(encoding))
    
    return processed

def get_status_url(payload):
    ''' Get Github status API URL from webhook payload.
    '''
    commit_sha = payload['head_commit']['id']
    status_url = payload['repository']['statuses_url']
    status_url = expand(status_url, dict(sha=commit_sha))
    
    current_app.logger.debug('Status URL {}'.format(status_url))
    
    return status_url

def post_status(status_url, status_json, github_auth):
    ''' POST status JSON to Github status API.
    '''
    posted = post(status_url, data=json.dumps(status_json), auth=github_auth,
                  headers={'Content-Type': 'application/json'})
    
    if posted.status_code not in range(200, 299):
        raise ValueError('Failed status post to {}'.format(status_url))
    
    if posted.json()['state'] != status_json['state']:
        raise ValueError('Mismatched status post to {}'.format(status_url))

def update_pending_status(status_url, files, github_auth):
    ''' Push pending status for head commit to Github status API.
    '''
    status = dict(context='openaddresses/hooked', state='pending',
                  description='Checking {}'.format(', '.join(files)))
    
    return post_status(status_url, status, github_auth)

def update_empty_status(status_url, github_auth):
    ''' Push success status for head commit to Github status API.
    '''
    status = dict(context='openaddresses/hooked', state='success',
                  description='Nothing to check')
    
    return post_status(status_url, status, github_auth)

def add_to_job_queue(files):
    '''
    '''
    queue_url = 'http://job-queue.openaddresses.io/jobs/'
    
    posted = post(queue_url, data=json.dumps(files), allow_redirects=True,
                  headers={'Content-Type': 'application/json'})
    
    if posted.status_code not in range(200, 299):
        raise ValueError('Failed status post to {}'.format(queue_url))

if __name__ == '__main__':
    app.run(debug=True)
