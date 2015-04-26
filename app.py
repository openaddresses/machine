from sys import stderr
from json import loads
from base64 import b64decode
from flask import Flask, request
from requests import get

app = Flask(__name__)

@app.route('/')
def index():
    return 'Yo.'

@app.route('/hook', methods=['GET', 'POST'])
def hook():
    #print >> stderr, request.method
    #print >> stderr, request.headers
    #print >> stderr, request.values
    #print >> stderr, repr(request.data)
    
    payload = loads(request.data)
    
    touched = set()
    
    for commit in payload['commits']:
        for filelist in (commit['added'], commit['modified']):
            touched.update(filelist)
        
    print >> stderr, 'Touched files', list(touched)
    
    commit_sha = payload['head_commit']['id']
    
    print >> stderr, 'Commit SHA', commit_sha
    
    for filename in touched:
        
        contents_url = payload['repository']['contents_url']
        contents_url = contents_url.replace('{+path}', filename)
        contents_url += '?ref={}'.format(commit_sha)
        
        print >> stderr, 'Contents URL', contents_url
        
        got = get(contents_url)
        
        content, blob_sha = got.json()['content'], got.json()['sha']
        
        print >> stderr, 'Contents SHA', blob_sha
        print >> stderr, repr(b64decode(content))

    return 'Yo.'

if __name__ == '__main__':
    app.run(debug=True)
