import logging; _L = logging.getLogger('openaddr.ci.webapi')

from urllib.parse import urljoin
import json, os

from flask import Blueprint, request, current_app, jsonify

from . import setup_logger, db_connect, db_cursor
from .objects import load_collection_zips_dict
from .webcommon import log_application_errors, nice_domain

webapi = Blueprint('webapi', __name__)

@webapi.route('/index.json')
@log_application_errors
def app_index_json():
    with db_connect(current_app.config['DATABASE_URL']) as conn:
        with db_cursor(conn) as db:
            zips = load_collection_zips_dict(db)
    
    collections = {}
    licenses = {'': 'Freely Shareable', 'sa': 'Share-Alike Required'}
    
    for ((collection, license), zip) in zips.items():
        if zip.content_length < 1024:
            # too small, probably empty
            continue

        if collection not in collections:
            collections[collection] = dict()
        
        if license not in collections[collection]:
            collections[collection][license] = dict()
        
        d = dict(url=nice_domain(zip.url), content_length=zip.content_length)
        d['license'] = licenses[license]
        collections[collection][license] = d
    
    return jsonify({
        'run_states_url': urljoin(request.url, u'/state.txt'),
        'latest_run_processed_url': urljoin(request.url, u'/latest/run/{source}.zip'),
        'collections': collections
        })

def apply_webapi_blueprint(app):
    '''
    '''
    app.register_blueprint(webapi)

    @app.before_first_request
    def app_prepare():
        setup_logger(os.environ.get('AWS_SNS_ARN'), logging.WARNING)
