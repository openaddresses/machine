import logging; _L = logging.getLogger('openaddr.ci.webapi')

import os, json, hmac, hashlib
from urllib.parse import urljoin, urlencode, urlunparse
from datetime import datetime, timedelta
from dateutil.tz import tzutc
from base64 import b64encode
from functools import wraps
from random import randint

from flask import (
    request, url_for, current_app, render_template, session, redirect, Blueprint
    )

from itsdangerous import URLSafeSerializer
import requests, uritemplate

from .. import compat
from . import setup_logger
from .webcommon import log_application_errors, flask_log_level

github_authorize_url = 'https://github.com/login/oauth/authorize{?state,client_id,redirect_uri,response_type,scope}'
github_exchange_url = 'https://github.com/login/oauth/access_token'
github_user_url = 'https://api.github.com/user'

USER_KEY = 'github user'
TOKEN_KEY = 'github token'

webauth = Blueprint('webauth', __name__)

def serialize(secret, data):
    return URLSafeSerializer(secret).dumps(data)

def unserialize(secret, data):
    return URLSafeSerializer(secret).loads(data)

def callback_url(request, callback_url):
    '''
    '''
    if 'X-Forwarded-Proto' in request.headers:
        _scheme = request.headers.get('X-Forwarded-Proto')
        scheme = _scheme.encode('utf8') if compat.PY2 else _scheme
        path = request.path.encode('utf8') if compat.PY2 else request.path

        base_url = urlunparse((scheme, request.host, path, None, None, None))
    else:
        base_url = request.url
    
    if compat.PY2 and hasattr(callback_url, 'encode'):
        callback_url = callback_url.encode('utf8')

    return urljoin(base_url, callback_url)

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
        if USER_KEY in session:
            session.pop(USER_KEY)
    
        if TOKEN_KEY in session:
            login, avatar_url, in_org = user_information(session[TOKEN_KEY])
            
            if login and in_org:
                session[USER_KEY] = dict(login=login, avatar_url=avatar_url)
            elif not login:
                session.pop(TOKEN_KEY)
                return render_template('oauth-hello.html', user_required=True,
                                       user=None, error_bad_login=True)
            elif not in_org:
                session.pop(TOKEN_KEY)
                return render_template('oauth-hello.html', user_required=True,
                                       user=None, error_org_membership=True)

        return untouched_route(*args, **kwargs)
    
    return wrapper

def s3_upload_form_fields(expires, bucketname, subdir, redirect_url, aws_secret):
    '''
    '''
    policy = {
        "expiration": expires.strftime('%Y-%m-%dT%H:%M:%SZ'),
        "conditions": [
            {"bucket": bucketname},
            ["starts-with", "$key", "cache/uploads/{}/".format(subdir)],
            {"acl": "public-read"},
            {"success_action_redirect": redirect_url},
            ["content-length-range", 16, 100 * 1024 * 1024]
        ]
        }
    
    policy_b64 = b64encode(json.dumps(policy).encode('utf8'))
    signature = hmac.new(aws_secret.encode('utf8'), policy_b64, hashlib.sha1)
    signature_b64 = b64encode(signature.digest())
    
    return dict(
        key=policy['conditions'][1][2] + '${filename}',
        acl=policy['conditions'][2]['acl'],
        policy=policy_b64.decode('utf8'),
        signature=signature_b64.decode('utf8')
        )

@webauth.route('/auth')
@update_authentication
@log_application_errors
def app_auth():
    return render_template('oauth-hello.html', user_required=True,
                           user=session.get(USER_KEY, {}))

@webauth.route('/auth/callback')
@log_application_errors
def app_callback():
    state = unserialize(current_app.secret_key, request.args['state'])

    token = exchange_tokens(request.args['code'],
                            current_app.config['GITHUB_OAUTH_CLIENT_ID'],
                            current_app.config['GITHUB_OAUTH_SECRET'])
    
    session[TOKEN_KEY] = token['access_token']
    
    return redirect(state.get('url', url_for('webauth.app_auth')), 302)

@webauth.route('/auth/login', methods=['POST'])
@log_application_errors
def app_login():
    state = serialize(current_app.secret_key,
                      dict(url=request.headers.get('Referer')))

    url = current_app.config.get('GITHUB_OAUTH_CALLBACK') or url_for('webauth.app_callback')
    args = dict(redirect_uri=callback_url(request, url), response_type='code', state=state)
    args.update(client_id=current_app.config['GITHUB_OAUTH_CLIENT_ID'])
    args.update(scope='user,public_repo,read:org')
    
    return redirect(uritemplate.expand(github_authorize_url, args), 303)

@webauth.route('/auth/logout', methods=['POST'])
@log_application_errors
def app_logout():
    if TOKEN_KEY in session:
        session.pop(TOKEN_KEY)
    
    if USER_KEY in session:
        session.pop(USER_KEY)
    
    return redirect(url_for('webauth.app_auth'), 302)

@webauth.route('/upload-cache')
@update_authentication
def app_upload_cache_data():
    '''
    '''
    if USER_KEY not in session:
        return render_template('upload-cache.html', user_required=True, user=None)
    
    random = hex(randint(0x100000, 0xffffff))[2:]
    subdir = '{login}/{0}'.format(random, **session[USER_KEY])
    expires = datetime.now(tz=tzutc()) + timedelta(minutes=5)

    redirect_url = callback_url(request, url_for('webauth.app_upload_cache_data'))
    bucketname, s3_secret = current_app.config['AWS_S3_BUCKET'], current_app.config['AWS_SECRET_ACCESS_KEY']
    fields = s3_upload_form_fields(expires, bucketname, subdir, redirect_url, s3_secret)
    
    fields.update(
        bucket=current_app.config['AWS_S3_BUCKET'],
        access_key=current_app.config['AWS_ACCESS_KEY_ID'],
        redirect=redirect_url,
        callback=request.args
        )
    
    return render_template('upload-cache.html', user_required=True,
                           user=session[USER_KEY], **fields)

def apply_webauth_blueprint(app):
    '''
    '''
    app.register_blueprint(webauth)
    
    # Use Github OAuth secret to sign Github login cookies too.
    app.secret_key = app.config['GITHUB_OAUTH_SECRET']

    @app.before_first_request
    def app_prepare():
        setup_logger(None,
                     None,
                     os.environ.get('AWS_SNS_ARN'),
                     flask_log_level(app.config))
