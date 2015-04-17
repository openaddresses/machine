from __future__ import absolute_import, division, print_function
import logging; _L = logging.getLogger('openaddr.process_all')

from .compat import standard_library

from argparse import ArgumentParser
from collections import defaultdict
from os.path import join, basename, relpath, splitext, dirname
from urllib.parse import urlparse
from io import BytesIO, TextIOWrapper
from datetime import datetime
from zipfile import ZipFile, ZIP_DEFLATED
from os import environ, close, remove
from tempfile import mkstemp
from json import dumps
from time import time
from glob import glob
import json

from . import paths, jobs, S3, render, summarize
from .compat import PY2, csvwriter, csvDictReader

parser = ArgumentParser(description='Run some source files.')

parser.add_argument('source_dir', nargs='?', default=paths.sources,
                    help='Optional source directory. Defaults to value of openaddr.paths.sources: {}.'.format(paths.sources))

parser.add_argument('bucketname',
                    help='Required S3 bucket name.')

parser.add_argument('-a', '--access-key', default=environ.get('AWS_ACCESS_KEY_ID', None),
                    help='Optional AWS access key name. Defaults to value of AWS_ACCESS_KEY_ID environment variable.')

parser.add_argument('-s', '--secret-key', default=environ.get('AWS_SECRET_ACCESS_KEY', None),
                    help='Optional AWS secret key name. Defaults to value of AWS_SECRET_ACCESS_KEY environment variable.')

parser.add_argument('-l', '--logfile', help='Optional log file name.')

parser.add_argument('-v', '--verbose', help='Turn on verbose logging',
                    action='store_const', dest='loglevel',
                    const=logging.DEBUG, default=logging.INFO)

parser.add_argument('-q', '--quiet', help='Turn off most logging',
                    action='store_const', dest='loglevel',
                    const=logging.WARNING, default=logging.INFO)

def main():
    args = parser.parse_args()
    
    jobs.setup_logger(logfile=args.logfile, log_level=args.loglevel)
    s3 = S3(args.access_key, args.secret_key, args.bucketname)
    
    #
    # Do the work
    #
    _L.info("Doing the work")
    run_name = '{:.3f}'.format(time())
    state = process(s3, args.source_dir, run_name)

    #
    # Prepare set of good sources
    #
    columns, rows = state[0], state[1:]
    all_sources = [dict(zip(columns, row)) for row in rows]
    good_sources = set([s['source'] for s in all_sources
                        if (s['cache'] and s['processed'])])
    
    #
    # Talk about the work
    #
    _L.info("Talking about the work")
    areas = (render.WORLD, 'world'), (render.USA, 'usa'), (render.EUROPE, 'europe')
    
    for (area, area_name) in areas:
        png_filename = 'render-{}-{}.png'.format(run_name, area_name)
        render.render(args.source_dir, good_sources, 960, 2, png_filename, area)
        render_data = open(png_filename).read()
        render_path = 'render-{}.png'.format(area_name)
        render_key = s3.new_key(join('runs', run_name, render_path))
        render_key.set_contents_from_string(render_data, policy='public-read',
                                            headers={'Content-Type': 'image/png'})

    png_usa_filename = 'render-{}-usa.png'.format(run_name)
    render.render(args.source_dir, good_sources, 960, 2, png_usa_filename, render.USA)
    render_data = open(png_usa_filename).read()
    render_key = s3.new_key(join('runs', run_name, 'render-usa.png'))
    render_key.set_contents_from_string(render_data, policy='public-read',
                                        headers={'Content-Type': 'image/png'})

    summary_html = summarize.summarize(s3, args.source_dir)
    summary_link = join('runs', run_name, 'index.html')
    summary_key = s3.new_key(summary_link)
    summary_key.set_contents_from_string(summary_html, policy='public-read',
                                         headers={'Content-Type': 'text/html'})

    index_html = '''<html>
        <head><meta http-equiv="refresh" content="0; url={0}"></head>
        <body><a href="{0}">{0}</a></body>
        </html>'''.format(summary_link)

    index_key = s3.new_key('index.html')
    index_key.set_contents_from_string(index_html, policy='public-read',
                  headers={'Cache-Control': 'non-cache, no-store, max-age=0',
                           'Content-Type': 'text/html'})

def read_state(s3, sourcedir):
    '''
    '''
    state_key = s3.get_key('state.txt')
    
    if state_key:
        state_link = state_key.get_contents_as_string()
        state_key = s3.get_key(state_link.strip())
    
    # Use default times of 'zzz' because we're pessimistic about the unknown.
    states = defaultdict(lambda: dict(cache_time='zzz', process_time='zzz'))

    if state_key:
        _L.debug('Found state in {}'.format(state_key.name))

        state_file = BytesIO(state_key.get_contents_as_string())
        rows = csvDictReader(state_file, dialect='excel-tab', encoding='utf-8')
        
        for row in rows:
            key = join(sourcedir, row['source'])
            states[key] = dict(cache=row['cache'],
                               version=row['version'],
                               fingerprint=row['fingerprint'],
                               cache_time=row['cache time'],
                               process_time=row['process time'])
    
    return states

def process(s3, sourcedir, run_name):
    ''' Return list of state data in JSON form from upload_states().
    '''
    # Find existing cache information
    source_extras = read_state(s3, sourcedir)
    _L.info('Loaded {} sources from state.txt'.format(len(source_extras)))
    
    # Cache data, if necessary
    source_files = glob(join(sourcedir, '*.json'))
    source_files.sort(key=lambda s: source_extras[s]['cache_time'], reverse=True)
    
    results = jobs.run_all_process_ones(source_files, 'out', source_extras)
    states = collect_states([path for path in results.values() if path])
    uploaded = upload_states(s3, states, run_name)
    
    return uploaded

def collect_states(result_paths):
    ''' Read a list of process_one.process() result paths, collect into one list.
    '''
    states = list()
    file_keys = 'cache', 'sample', 'processed', 'output'
    
    for result_path in sorted(result_paths):
        with open(result_path) as result_file:
            columns, values = json.load(result_file)
            state = dict(zip(columns, values))
    
        if len(states) == 0:
            states.append(columns)
        
        for key in file_keys:
            resource = state[key]
            if resource and urlparse(resource).scheme in ('file', ''):
                state[key] = join(dirname(result_path), urlparse(resource).path)
        
        states.append([state[key] for key in columns])
    
    return states

def upload_file(s3, keyname, filename):
    ''' Create a new S3 key with filename contents, return its URL and MD5 hash.
    '''
    key = s3.new_key(keyname)

    kwargs = dict(policy='public-read', reduced_redundancy=True)
    key.set_contents_from_filename(filename, **kwargs)
    url = key.generate_url(expires_in=0, query_auth=False, force_http=True)
    
    return url, key.md5

def package_result(source, processed_path):
    '''
    '''
    _, ext = splitext(processed_path)
    handle, zip_path = mkstemp(prefix=source, suffix='.zip')
    close(handle)
    
    zip_file = ZipFile(zip_path, mode='w', compression=ZIP_DEFLATED)

    if ext == '.csv':
        # Add virtual format to make CSV readable by QGIS, OGR, etc.
        # More information: http://www.gdal.org/drv_vrt.html
        template = join(dirname(__file__), 'templates', 'conform-result.vrt')
        with open(template) as file:
            content = file.read().format(source=source)
            zip_file.writestr(source + '.vrt', content)
    
    zip_file.write(processed_path, source + ext)
    zip_file.close()
    
    return zip_path

def upload_states(s3, states, run_name):
    ''' Return list of state data in JSON form.
    '''
    _L.info("Uploading states")
    columns = states[0]
    new_states = states[:1]
    
    for values in states[1:]:
        state = dict(zip(columns, values))
        source, _ = splitext(state['source'])
        _L.debug('Uploading files for {}'.format(source))
        
        yyyymmdd = datetime.utcnow().strftime('%Y%m%d')
        
        if urlparse(state['cache'] or '').scheme in ('http', 'https') and state['fingerprint']:
            # Do not re-upload an existing remote cached file.
            pass
        elif state['cache']:
            # e.g. /20141204/us-wa-king.zip
            _, cache_ext = splitext(state['cache'])
            key_name = '/{}/{}{}'.format(yyyymmdd, source, cache_ext)
            state['cache'], state['fingerprint'] = upload_file(s3, key_name, state['cache'])
            state['version'] = yyyymmdd
    
        if state['sample']:
            # e.g. /20141226/samples/za-wc-cape_town.json
            _, sample_ext = splitext(state['sample'])
            key_name = '/{}/samples/{}{}'.format(yyyymmdd, source, sample_ext)
            state['sample'], _ = upload_file(s3, key_name, state['sample'])
    
        if state['processed']:
            # e.g. /us-tx-denton.csv
            key_name = '/{}.zip'.format(source)
            zip_path = package_result(source, state['processed'])
            state['processed'], _ = upload_file(s3, key_name, zip_path)
            remove(zip_path)
    
        if state['output']:
            # e.g. /runs/<run name>/us-tx-denton.txt
            _, output_ext = splitext(state['output'])
            key_name = '/runs/{}/{}{}'.format(run_name, source, output_ext)
            state['output'], _ = upload_file(s3, key_name, state['output'])
        
        new_states.append([state[col] for col in columns])
    
    state_file = BytesIO()

    if PY2:
        out = csvwriter(state_file, dialect='excel-tab', encoding='utf-8')
    else:
        out = csvwriter(TextIOWrapper(state_file, encoding='utf-8'), dialect='excel-tab')
    
    for state in new_states:
        out.writerow(state)

    state_data = state_file.getvalue()
    state_path = join('runs', run_name, 'state.txt')
    state_args = dict(policy='public-read', headers={'Content-Type': 'text/plain'})

    s3.new_key(state_path).set_contents_from_string(state_data, **state_args)
    s3.new_key('state.txt').set_contents_from_string(state_path, **state_args)
    
    json_data = dumps(new_states, indent=2)
    json_path = join('runs', run_name, 'state.json')
    json_args = dict(policy='public-read', headers={'Content-Type': 'application/json'})
    
    s3.new_key(json_path).set_contents_from_string(json_data, **json_args)
    s3.new_key('state.json').set_contents_from_string(dumps(json_path), **json_args)
    
    _L.info('Wrote {} sources to state'.format(len(new_states) - 1))
    
    # Gather the one big zip
    bytes = BytesIO()
    archive = ZipFile(bytes, mode='w', compression=ZIP_DEFLATED)
    
    for state in states[1:]:
        d = dict(zip(columns, state))

        if not d['processed']:
            continue
    
        (source, _), (_, ext) = splitext(d['source']), splitext(d['processed'])
        archive.write(d['processed'], source + ext)
    
    archive.close()
    zip_key = s3.new_key('openaddresses-complete.zip')
    zip_args = dict(policy='public-read', headers={'Content-Type': 'application/zip'})
    zip_key.set_contents_from_string(bytes.getvalue(), **zip_args)
    
    return new_states

if __name__ == '__main__':
    exit(main())
