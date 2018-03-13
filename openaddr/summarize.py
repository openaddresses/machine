from __future__ import absolute_import, division, print_function

import json
from csv import DictReader
from io import StringIO
from base64 import b64decode
from operator import itemgetter
from os.path import join, dirname, splitext, relpath
from dateutil.parser import parse as parse_datetime
from urllib.parse import urljoin
from os import environ
from re import compile
import json, pickle

import requests
from uritemplate import expand as expand_uri

from . import S3, __version__

# Sort constants for summarize_runs()
GLASS_HALF_FULL = 1
GLASS_HALF_EMPTY = 2

def _get_cached(memcache, key):
    ''' Get a thing from the cache, or None.
    '''
    if not memcache:
        return None

    pickled = memcache.get(key)

    if pickled is None:
        return None

    try:
        value = pickle.loads(pickled)
    except Exception as e:
        return None
    else:
        return value

def _set_cached(memcache, key, value):
    ''' Put a thing in the cache, if it exists.
    '''
    if not memcache:
        return

    pickled = pickle.dumps(value, protocol=2)
    memcache.set(key, pickled)

def is_coverage_complete(source):
    '''
    '''
    if 'coverage' in source:
        cov = source['coverage']
        if ('ISO 3166' in cov or 'US Census' in cov or 'geometry' in cov):
            return True

    return False

def state_conform_type(state):
    '''
    '''
    if 'cache' not in state.keys:
        return None

    if state.cache is None:
        return None

    if state.cache.endswith('.zip'):
        if state.geometry_type in ('Polygon', 'MultiPolygon'):
            return 'shapefile-polygon'
        else:
            return 'shapefile'
    elif state.cache.endswith('.json'):
        return 'geojson'
    elif state.cache.endswith('.csv'):
        return 'csv'
    else:
        return None

def convert_run(memcache, run, url_template):
    '''
    '''
    cache_key = 'converted-run-{}-{}'.format(run.id, __version__)
    cached_run = _get_cached(memcache, cache_key)
    if cached_run is not None:
        return cached_run

    try:
        source = json.loads(b64decode(run.source_data).decode('utf8'))
    except:
        source = {}

    run_state = run.state or {}

    converted_run = {
        'address count': run_state.address_count,
        'cache': run_state.cache,
        'cache time': run_state.cache_time,
        'cache_date': run.datetime_tz.strftime('%Y-%m-%d'),
        'conform': bool(source.get('conform', False)),
        'conform type': state_conform_type(run_state),
        'coverage complete': is_coverage_complete(source),
        'fingerprint': run_state.fingerprint,
        'geometry type': run_state.geometry_type,
        'href': expand_uri(url_template, run.__dict__),
        'output': run_state.output,
        'process time': run_state.process_time,
        'processed': run_state.processed,
        'sample': run_state.sample,
        'run_id': run.id,
        'shortname': splitext(relpath(run.source_path, 'sources'))[0],
        'skip': bool(source.get('skip', False)),
        'source': relpath(run.source_path, 'sources'),
        'type': source.get('type', '').lower(),
        'version': run_state.version,
        'source problem': run_state.source_problem
        }

    _set_cached(memcache, cache_key, converted_run)
    return converted_run

def run_counts(runs):
    '''
    '''
    states = [(run.state or {}) for run in runs]

    return {
        'sources': len(runs),
        'cached': sum([int(bool(state.cache)) for state in states]),
        'processed': sum([int(bool(state.processed)) for state in states]),
        'addresses': sum([int(state.address_count or 0) for state in states])
        }

def sort_run_dicts(dicts, sort_order):
    '''
    '''
    if sort_order is GLASS_HALF_FULL:
        # Put the happy, successful stuff up front.
        key = lambda d: (not bool(d['processed']), not bool(d['cache']), d['source'])

    elif sort_order is GLASS_HALF_EMPTY:
        # Put the stuff that needs help up front.
        key = lambda d: (bool(d['cache']), bool(d['processed']), d['source'])

    else:
        raise ValueError('Unknown sort order "{}"'.format(sort_order))

    dicts.sort(key=key)

def nice_integer(number):
    ''' Format a number like '999,999,999'
    '''
    string = str(number)
    pattern = compile(r'^(\d+)(\d\d\d)\b')

    while pattern.match(string):
        string = pattern.sub(r'\1,\2', string)

    return string

def break_state(string):
    ''' Adds <wbr> tag and returns an HTML-safe string.
    '''
    pattern = compile(r'^(.+)/([^/]+)$')
    string = string.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

    if pattern.match(string):
        string = pattern.sub(r'\1/<wbr>\2', string)

    return string

def summarize_runs(memcache, runs, datetime, owner, repository, sort_order):
    ''' Return summary data for set.html template.
    '''
    base_url = expand_uri(u'https://github.com/{owner}/{repository}/',
                          dict(owner=owner, repository=repository))
    url_template = urljoin(base_url, u'blob/{commit_sha}/{+source_path}')

    states = [convert_run(memcache, run, url_template) for run in runs]
    counts = run_counts(runs)
    sort_run_dicts(states, sort_order)

    return dict(states=states, last_modified=datetime, counts=counts)
