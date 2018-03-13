import logging; _L = logging.getLogger('openaddr.ci.webapi')

from urllib.parse import urljoin
from operator import attrgetter
from collections import defaultdict
import json, os, csv, io

from flask import Response, Blueprint, request, current_app, jsonify, url_for, redirect
from flask_cors import CORS

from .objects import (
    load_collection_zips_dict, read_latest_set, read_completed_runs_to_date,
    read_completed_set_runs, read_set
    )

from . import setup_logger, db_connect, db_cursor, tileindex
from .webcommon import log_application_errors, nice_domain, flask_log_level

CSV_HEADER = 'source', 'cache', 'sample', 'geometry type', 'address count', \
             'version', 'fingerprint', 'cache time', 'processed', 'process time', \
             'process hash', 'output', 'attribution required', 'attribution name', \
             'share-alike', 'code version'

webapi = Blueprint('webapi', __name__)
CORS(webapi)

@webapi.route('/index.json')
@log_application_errors
def app_index_json():
    with db_connect(current_app.config['DATABASE_URL']) as conn:
        with db_cursor(conn) as db:
            zips = load_collection_zips_dict(db)
            set = read_latest_set(db, 'openaddresses', 'openaddresses')

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

    run_states_url = url_for('webapi.app_get_state_txt')
    latest_run_processed_url = url_for('webhooks.app_get_latest_run', source='____').replace('____', '{source}')
    tileindex_url = url_for('webapi.app_get_tileindex_zip', lon='xxx', lat='yyy').replace('xxx', '{lon}').replace('yyy', '{lat}')
    licenses_url = url_for('webapi.app_licenses_json')
    latest_set_url = url_for('webapi.app_get_set_data', set_id=set.id)

    render_world_url = 'https://s3.amazonaws.com/{}/render-world.png'.format(current_app.config['AWS_S3_BUCKET'])
    render_europe_url = 'https://s3.amazonaws.com/{}/render-europe.png'.format(current_app.config['AWS_S3_BUCKET'])
    render_usa_url = 'https://s3.amazonaws.com/{}/render-usa.png'.format(current_app.config['AWS_S3_BUCKET'])
    render_geojson_url = 'https://s3.amazonaws.com/{}/render-world.geojson'.format(current_app.config['AWS_S3_BUCKET'])

    return jsonify({
        'run_states_url': urljoin(request.url, run_states_url),
        'latest_run_processed_url': urljoin(request.url, latest_run_processed_url),
        'tileindex_url': urljoin(request.url, tileindex_url),
        'licenses_url': urljoin(request.url, licenses_url),
        'latest_set_url': urljoin(request.url, latest_set_url),
        'render_world_url': nice_domain(render_world_url),
        'render_europe_url': nice_domain(render_europe_url),
        'render_usa_url': nice_domain(render_usa_url),
        'render_geojson_url': nice_domain(render_geojson_url),
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
        if run_state.attribution_required != 'false':
            attribution = run_state.attribution_name

        key = run_state.license, attribution
        licenses[key].append((source, run_state.website))

    licenses = [dict(license=lic, attribution=attr, sources=sorted(srcs))
                for ((lic, attr), srcs) in sorted(licenses.items(), key=repr)]

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

    buffer = io.StringIO()
    output = csv.DictWriter(buffer, CSV_HEADER, dialect='excel-tab')
    output.writerow({col: col for col in CSV_HEADER})
    for run in sorted(runs, key=attrgetter('source_path')):
        run_state = run.state or {}
        row = {col: run_state.get(col) for col in CSV_HEADER}
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
            runs = read_completed_set_runs(db, set_id)

    buffer = io.StringIO()
    output = csv.DictWriter(buffer, CSV_HEADER, dialect='excel-tab')
    output.writerow({col: col for col in CSV_HEADER})
    for run in sorted(runs, key=attrgetter('source_path')):
        run_state = run.state or {}
        row = {col: run_state.get(col) for col in CSV_HEADER}
        row['source'] = os.path.relpath(run.source_path, 'sources')
        row['code version'] = run.code_version
        row['cache'] = nice_domain(row['cache'])
        row['sample'] = nice_domain(row['sample'])
        row['processed'] = nice_domain(row['processed'])
        row['output'] = nice_domain(row['output'])
        output.writerow(row)

    return Response(buffer.getvalue(),
                    headers={'Content-Type': 'text/plain; charset=utf8'})

@webapi.route('/sets/<set_id>.json', methods=['GET'])
@log_application_errors
def app_get_set_data(set_id):
    '''
    '''
    with db_connect(current_app.config['DATABASE_URL']) as conn:
        with db_cursor(conn) as db:
            set = read_set(db, set_id)

    return jsonify({
        'id': set.id,
        'commit_sha': set.commit_sha,
        'datetime_start': str(set.datetime_start),
        'datetime_end': str(set.datetime_end),

        'render_world_url': nice_domain(set.render_world),
        'render_europe_url': nice_domain(set.render_europe),
        'render_usa_url': nice_domain(set.render_usa),
        'render_geojson_url': nice_domain(set.render_geojson),
        })

@webapi.route('/tiles/<lon>/<lat>.zip', methods=['GET'])
@log_application_errors
def app_get_tileindex_zip(lon, lat):
    '''
    '''
    try:
        key = tileindex.lonlat_key(float(lon), float(lat))
    except ValueError:
        return Response('"{}" and "{}" must both be numeric.\n'.format(lon, lat), status=404)

    if not (-180 <= key[0] <= 180 and -90 <= key[1] <= 90):
        return Response('"{}" and "{}" must both be on earth.\n'.format(lon, lat), status=404)

    bucket = current_app.config['AWS_S3_BUCKET']
    url = u'https://s3.amazonaws.com/{}/tiles/{:.1f}/{:.1f}.zip'.format(bucket, *key)
    return redirect(nice_domain(url), 302)

def apply_webapi_blueprint(app):
    '''
    '''
    app.register_blueprint(webapi)

    @app.before_first_request
    def app_prepare():
        setup_logger(os.environ.get('AWS_SNS_ARN'), None, flask_log_level(app.config))
