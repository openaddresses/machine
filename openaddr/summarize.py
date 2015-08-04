from __future__ import absolute_import, division, print_function
from .compat import standard_library

import json
from csv import DictReader
from io import StringIO
from base64 import b64decode
from operator import itemgetter
from os.path import join, dirname, splitext, relpath
from dateutil.parser import parse as parse_datetime
from uritemplate import expand
from os import environ
from re import compile
import json

from jinja2 import Environment, FileSystemLoader
from requests import get

from . import S3, paths

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
    if 'cache' not in state:
        return None
    
    if state['cache'] is None:
        return None
    
    if state['cache'].endswith('.zip'):
        if state.get('geometry type', 'Point') in ('Polygon', 'MultiPolygon'):
            return 'shapefile-polygon'
        else:
            return 'shapefile'
    elif state['cache'].endswith('.json'):
        return 'geojson'
    elif state['cache'].endswith('.csv'):
        return 'csv'
    else:
        return None

def convert_run(run, url_template):
    '''
    '''
    try:
        source = json.loads(b64decode(run.source_data).decode('utf8'))
    except:
        source = {}
    
    try:
        sample_data = get(run.state.get('sample')).json()
    except:
        sample_data = None
    
    run_state = run.state or {}
    
    return {
        'address count': run_state.get('address count'),
        'cache': run_state.get('cache'),
        'cache time': run_state.get('cache time'),
        'cache_date': run.datetime_tz.strftime('%Y-%m-%d'),
        'conform': bool(source.get('conform', False)),
        'conform type': state_conform_type(run_state),
        'coverage complete': is_coverage_complete(source),
        'fingerprint': run_state.get('fingerprint'),
        'geometry type': run_state.get('geometry type'),
        'href': expand(url_template, run.__dict__),
        'output': run_state.get('output'),
        'process time': run_state.get('process time'),
        'processed': run_state.get('processed'),
        'sample': run_state.get('sample'),
        'sample_data': sample_data,
        'shortname': splitext(relpath(run.source_path, 'sources'))[0],
        'skip': bool(source.get('skip', False)),
        'source': relpath(run.source_path, 'sources'),
        'type': source.get('type', '').lower(),
        'version': run_state.get('version')
        }

def run_counts(runs):
    '''
    '''
    states = [(run.state or {}) for run in runs]
    
    return {
        'sources': len(runs),
        'cached': sum([int(bool(state.get('cache'))) for state in states]),
        'processed': sum([int(bool(state.get('processed'))) for state in states]),
        'addresses': sum([int(state.get('address count') or 0) for state in states])
        }

def sort_run_dicts(dicts):
    '''
    '''
    dicts.sort(key=lambda d: (bool(d['cache']), bool(d['processed']), d['source']))

def load_states(s3, source_dir):
    # Find existing cache information
    state_key = s3.get_key('state.txt')
    states, counts = list(), dict(processed=0, cached=0, sources=0, addresses=0)

    if state_key:
        state_link = state_key.get_contents_as_string()
        if b'\t' not in state_link:
            # it's probably a link to someplace else.
            state_key = s3.get_key(state_link.strip())
    
    if state_key:
        last_modified = parse_datetime(state_key.last_modified)
        state_file = StringIO(state_key.get_contents_as_string().decode('utf8'))
        
        for row in DictReader(state_file, dialect='excel-tab'):
            row['shortname'], _ = splitext(row['source'])
            row['href'] = row['processed'] or row['cache'] or None
            row['href'] = 'https://github.com/openaddresses/openaddresses/blob/master/sources/' + row['source']
            
            if row.get('version', False):
                v = row['version']
                row['cache_date'] = '{}-{}-{}'.format(v[0:4], v[4:6], v[6:8])
            else:
                row['cache_date'] = None

            counts['sources'] += 1
            counts['cached'] += 1 if row['cache'] else 0
            counts['processed'] += 1 if row['processed'] else 0
            counts['addresses'] += int(row['address count'] or 0)

            with open(join(source_dir, row['source'])) as file:
                data = json.load(file)
            
                row['type'] = data.get('type', '').lower()
                row['conform'] = bool(data.get('conform', False))
                row['skip'] = bool(data.get('skip', False))
            
            if row.get('sample', False):
                row['sample_data'] = get(row['sample']).json()
            
            if not row.get('sample_data', False):
                row['sample_data'] = list()
            
            row['conform type'] = state_conform_type(row)
            row['coverage complete'] = is_coverage_complete(data)
            
            states.append(row)
    
    sort_run_dicts(states)
    
    return last_modified, states, counts

def nice_integer(number):
    ''' Format a number like '999,999,999'
    '''
    string = str(number)
    pattern = compile(r'^(\d+)(\d\d\d)\b')
    
    while pattern.match(string):
        string = pattern.sub(r'\1,\2', string)
    
    return string

def main():
    s3 = S3(environ['AWS_ACCESS_KEY_ID'], environ['AWS_SECRET_ACCESS_KEY'], 'data-test.openaddresses.io')
    print(summarize(s3, paths.sources).encode('utf8'))

def summarize(s3, source_dir):
    ''' Return summary HTML.
    '''
    env = Environment(loader=FileSystemLoader(join(dirname(__file__), 'templates')))
    env.filters['tojson'] = lambda value: json.dumps(value, ensure_ascii=False)
    env.filters['nice_integer'] = nice_integer
    template = env.get_template('state.html')

    last_modified, states, counts = load_states(s3, source_dir)
    from pprint import pprint
    pprint(last_modified)
    pprint(states)
    pprint(counts)
    return b''
    return template.render(states=states, last_modified=last_modified, counts=counts)

if __name__ == '__main__':
    exit(main())
