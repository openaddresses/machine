import logging; _L = logging.getLogger('openaddr.ci.webapi')

import os

from flask import Blueprint

from . import setup_logger
from .webcommon import log_application_errors

webauth = Blueprint('webauth', __name__)

@webauth.route('/auth')
@log_application_errors
def app_auth():
    return 'Yo.'

def apply_webauth_blueprint(app):
    '''
    '''
    app.register_blueprint(webauth)

    @app.before_first_request
    def app_prepare():
        setup_logger(os.environ.get('AWS_ACCESS_KEY_ID'),
                     os.environ.get('AWS_SECRET_ACCESS_KEY'),
                     os.environ.get('AWS_SNS_ARN'), logging.WARNING)
