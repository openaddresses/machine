import logging; _L = logging.getLogger('openaddr.ci.webapi')

import os
from urllib.parse import urljoin, urlencode

from flask import (
    request, url_for, current_app, render_template, session, redirect, Blueprint
    )

from itsdangerous import URLSafeSerializer
import requests

from . import setup_logger
from .webcommon import log_application_errors

github_authorize_url = 'https://github.com/login/oauth/authorize'
github_exchange_url = 'https://github.com/login/oauth/access_token'

webauth = Blueprint('webauth', __name__)

def serialize(secret, data):
    return URLSafeSerializer(secret).dumps(data)

def unserialize(secret, data):
    return URLSafeSerializer(secret).loads(data)

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

@webauth.route('/auth')
@log_application_errors
def app_auth():
    state = serialize(current_app.config['GITHUB_OAUTH_SECRET'],
                      dict(url=request.url))
    
    args = dict(redirect_uri=urljoin(request.url, url_for('webauth.app_callback')))
    args.update(client_id=current_app.config['GITHUB_OAUTH_CLIENT_ID'], state=state)
    args.update(response_type='code')
    
    return render_template('oauth-hello.html', href=github_authorize_url, **args)
    
    return str(dict(args))

    return 'Yo.'

@webauth.route('/auth/callback')
@log_application_errors
def app_callback():
    state = unserialize(current_app.config['GITHUB_OAUTH_SECRET'],
                        request.args['state'])

    token = exchange_tokens(request.args['code'],
                            current_app.config['GITHUB_OAUTH_CLIENT_ID'],
                            current_app.config['GITHUB_OAUTH_SECRET'])
    
    session['github token'] = token['access_token']
    
    return render_template('oauth-success.html', url=state.get('url'),
                           href=url_for('webauth.app_logout'))
    
    return request.args['state'] + '\n' + str(state)
    return 'Yo.'

@webauth.route('/auth/logout', methods=['POST'])
@log_application_errors
def app_logout():
    if 'github token' in session:
        session.pop('github token')
    
    return redirect(url_for('webauth.app_auth'), 302)

def apply_webauth_blueprint(app):
    '''
    '''
    app.register_blueprint(webauth)
    app.secret_key = 'poop'

    @app.before_first_request
    def app_prepare():
        setup_logger(os.environ.get('AWS_ACCESS_KEY_ID'),
                     os.environ.get('AWS_SECRET_ACCESS_KEY'),
                     os.environ.get('AWS_SNS_ARN'), logging.WARNING)
