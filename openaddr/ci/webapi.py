import logging; _L = logging.getLogger('openaddr.ci.webapi')

from urllib.parse import urljoin
from operator import attrgetter
from collections import defaultdict
import json, os

from flask import Response, Blueprint, request, current_app, jsonify
from flask.ext.cors import CORS

from .objects import (
    load_collection_zips_dict, read_latest_set, read_completed_runs_to_date,
    new_read_completed_set_runs
    )

from . import setup_logger, db_connect, db_cursor
from .webcommon import log_application_errors, nice_domain
from ..compat import expand_uri, csvIO, csvDictWriter

CSV_HEADER = 'source', 'cache', 'sample', 'geometry type', 'address count', \
             'version', 'fingerprint', 'cache time', 'processed', 'process time', \
             'output', 'attribution required', 'attribution name', 'share-alike', \
             'code version'

webapi = Blueprint('webapi', __name__)
CORS(webapi)

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
        'licenses_url': urljoin(request.url, u'/latest/licenses.json'),
        'collections': collections
        })

@webapi.route('/latest/licenses.json')
@log_application_errors
def app_licenses_json():
    with db_connect(current_app.config['DATABASE_URL']) as conn:
        with db_cursor(conn) as db:
            set = read_latest_set(db, 'openaddresses', 'openaddresses')
            runs = read_completed_runs_to_date(db, set.id)

    licenses = defaultdict(list)
    
    for run in runs:
        run_state = run.state or {}
        source = os.path.relpath(run.source_path, 'sources')
    
        attribution = None
        if run_state.get('attribution required') != 'false':
            attribution = run_state.get('attribution name')
        
        key = run_state.get('license'), attribution
        licenses[key].append((source, run_state.get('website')))
        
    licenses = [dict(license=lic, attribution=attr, sources=sorted(srcs))
                for ((lic, attr), srcs) in sorted(licenses.items())]
    
    return jsonify(licenses=licenses)

@webapi.route('/state.txt', methods=['GET'])
@log_application_errors
def app_get_state_txt():
    '''
    '''
    with db_connect(current_app.config['DATABASE_URL']) as conn:
        with db_cursor(conn) as db:
            set = read_latest_set(db, 'openaddresses', 'openaddresses')
            runs = read_completed_runs_to_date(db, set.id)
    
    buffer = csvIO()
    output = csvDictWriter(buffer, CSV_HEADER, dialect='excel-tab', encoding='utf8')
    output.writerow({col: col for col in CSV_HEADER})
    for run in sorted(runs, key=attrgetter('source_path')):
        run_state = run.state or {}
        row = {col: run_state.get(col, None) for col in CSV_HEADER}
        row['source'] = os.path.relpath(run.source_path, 'sources')
        row['code version'] = run.code_version
        row['cache'] = nice_domain(row['cache'])
        row['sample'] = nice_domain(row['sample'])
        row['processed'] = nice_domain(row['processed'])
        row['output'] = nice_domain(row['output'])
        output.writerow(row)

    return Response(buffer.getvalue(),
                    headers={'Content-Type': 'text/plain; charset=utf8'})

@webapi.route('/sets/<set_id>/state.txt', methods=['GET'])
@log_application_errors
def app_get_set_state_txt(set_id):
    '''
    '''
    with db_connect(current_app.config['DATABASE_URL']) as conn:
        with db_cursor(conn) as db:
            runs = new_read_completed_set_runs(db, set_id)
    
    buffer = csvIO()
    output = csvDictWriter(buffer, CSV_HEADER, dialect='excel-tab', encoding='utf8')
    output.writerow({col: col for col in CSV_HEADER})
    for run in sorted(runs, key=attrgetter('source_path')):
        run_state = run.state or {}
        row = {col: run_state.get(col, None) for col in CSV_HEADER}
        row['source'] = os.path.relpath(run.source_path, 'sources')
        row['code version'] = run.code_version
        row['cache'] = nice_domain(row['cache'])
        row['sample'] = nice_domain(row['sample'])
        row['processed'] = nice_domain(row['processed'])
        row['output'] = nice_domain(row['output'])
        output.writerow(row)

    return Response(buffer.getvalue(),
                    headers={'Content-Type': 'text/plain; charset=utf8'})

def apply_webapi_blueprint(app):
    '''
    '''
    app.register_blueprint(webapi)

    @app.before_first_request
    def app_prepare():
        setup_logger(os.environ.get('AWS_SNS_ARN'), logging.WARNING)
