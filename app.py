from os.path import relpath, splitext
from base64 import b64decode
import json, os

from flask import Flask, request, Response, current_app
from uritemplate import expand
from requests import get

app = Flask(__name__)
app.config['GITHUB_AUTH'] = os.environ['GITHUB_TOKEN'], 'x-oauth-basic'

@app.route('/')
def index():
    return 'Yo.'

@app.route('/hook', methods=['GET', 'POST'])
def hook():
    github_auth = current_app.config['GITHUB_AUTH']
    files = process_payload(json.loads(request.data), github_auth)
    
    serialize = lambda data: json.dumps(data, indent=2, sort_keys=True)
    response = '\n\n'.join(['{}:\n\n{}\n'.format(name, serialize(data))
                            for (name, data) in sorted(files.items())])
    
    return Response(response, headers={'Content-Type': 'text/plain'})

def get_touched_files(payload):
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

def process_payload(payload, github_auth):
    ''' Return a dictionary of file paths and decoded JSON contents.
    '''
    processed, touched = dict(), get_touched_files(payload)
    
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
            processed[filename] = json.loads(b64decode(content))
        else:
            raise ValueError('Unrecognized encoding "{}"'.format(encoding))
    
    return processed

if __name__ == '__main__':
    app.run(debug=True)
