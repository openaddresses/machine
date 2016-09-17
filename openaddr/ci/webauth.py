import logging; _L = logging.getLogger('openaddr.ci.webapi')

import os
from urllib.parse import urljoin, urlencode, urlunparse
from functools import wraps

from flask import (
    request, url_for, current_app, render_template, session, redirect, Blueprint
    )

from itsdangerous import URLSafeSerializer
import requests, uritemplate

from .. import compat
from . import setup_logger
from .webcommon import log_application_errors

github_authorize_url = 'https://github.com/login/oauth/authorize{?state,client_id,redirect_uri,response_type}'
github_exchange_url = 'https://github.com/login/oauth/access_token'
github_user_url = 'https://api.github.com/user'

webauth = Blueprint('webauth', __name__)

def serialize(secret, data):
    return URLSafeSerializer(secret).dumps(data)

def unserialize(secret, data):
    return URLSafeSerializer(secret).loads(data)

def correct_url(request):
    path = request.path.encode('utf8') if compat.PY2 else request.path

    if 'X-Forwarded-Proto' not in request.headers:
        return request.url

    _scheme = request.headers.get('X-Forwarded-Proto')
    scheme = _scheme.encode('utf8') if compat.PY2 else _scheme
    actual_url = urlunparse((scheme, request.host, path, None, None, None))

def exchange_tokens(code, client_id, secret):
    ''' Exchange the temporary code for an access token

        http://developer.github.com/v3/oauth/#parameters-1
    '''
    data = dict(client_id=client_id, code=code, client_secret=secret)
    resp = requests.post(github_exchange_url, urlencode(data),
                         headers={'Accept': 'application/json'})
    auth = resp.json()

    if 'error' in auth:
        raise RuntimeError('Github said "{error}".'.format(**auth))
    
    elif 'access_token' not in auth:
        raise RuntimeError("missing `access_token`.")
    
    return auth

def user_information(token, org_id=6895392):
    '''
    '''
    header = {'Authorization': 'token {}'.format(token)}
    resp1 = requests.get(github_user_url, headers=header)
    
    if resp1.status_code != 200:
        return None, None, None

    login, avatar_url = resp1.json().get('login'), resp1.json().get('avatar_url')
    
    orgs_url = resp1.json().get('organizations_url')
    resp2 = requests.get(orgs_url, headers=header)
    org_ids = [org['id'] for org in resp2.json()]
    
    return login, avatar_url, bool(org_id in org_ids)

def update_authentication(untouched_route):
    '''
    '''
    @wraps(untouched_route)
    def wrapper(*args, **kwargs):
        # remove this always
        if 'github user' in session:
            session.pop('github user')
    
        if 'github token' in session:
            login, avatar_url, in_org = user_information(session['github token'])
            
            if login and in_org:
                session['github user'] = dict(login=login, avatar_url=avatar_url)

        return untouched_route(*args, **kwargs)
    
    return wrapper

@webauth.route('/auth')
@update_authentication
@log_application_errors
def app_auth():
    return render_template('oauth-hello.html', user=session.get('github user', {}))

@webauth.route('/auth/callback')
@log_application_errors
def app_callback():
    state = unserialize(current_app.secret_key, request.args['state'])

    token = exchange_tokens(request.args['code'],
                            current_app.config['GITHUB_OAUTH_CLIENT_ID'],
                            current_app.config['GITHUB_OAUTH_SECRET'])
    
    session['github token'] = token['access_token']
    
    return redirect(state.get('url', url_for('webauth.app_auth')), 302)

@webauth.route('/auth/login', methods=['POST'])
@log_application_errors
def app_login():
    state = serialize(current_app.secret_key,
                      dict(url=request.headers.get('Referer')))

    _url = url_for('webauth.app_callback')
    redirect_url = _url.decode('utf8') if compat.PY2 else _url

    args = dict(redirect_uri=urljoin(correct_url(request), redirect_url))
    args.update(client_id=current_app.config['GITHUB_OAUTH_CLIENT_ID'])
    args.update(response_type='code', state=state)
    
    return redirect(uritemplate.expand(github_authorize_url, args), 303)

@webauth.route('/auth/logout', methods=['POST'])
@log_application_errors
def app_logout():
    if 'github token' in session:
        session.pop('github token')
    
    if 'github user' in session:
        session.pop('github user')
    
    return redirect(url_for('webauth.app_auth'), 302)

def apply_webauth_blueprint(app):
    '''
    '''
    app.register_blueprint(webauth)
    
    # Use Github OAuth secret to sign Github login cookies too.
    app.secret_key = app.config['GITHUB_OAUTH_SECRET']

    @app.before_first_request
    def app_prepare():
        setup_logger(os.environ.get('AWS_ACCESS_KEY_ID'),
                     os.environ.get('AWS_SECRET_ACCESS_KEY'),
                     os.environ.get('AWS_SNS_ARN'), logging.WARNING)
