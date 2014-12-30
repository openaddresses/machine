from __future__ import absolute_import, division, print_function
from future import standard_library; standard_library.install_aliases()

from argparse import ArgumentParser
from collections import defaultdict
from os.path import join, basename, relpath, splitext, dirname
from csv import writer, DictReader
from io import BytesIO
from logging import getLogger
from os import environ
from json import dumps
from time import time
from glob import glob
import json

from . import paths, jobs, S3, render, summarize

parser = ArgumentParser(description='Run some source files.')

parser.add_argument('bucketname',
                    help='Required S3 bucket name.')

parser.add_argument('-a', '--access-key', default=environ.get('AWS_ACCESS_KEY_ID', None),
                    help='Optional AWS access key name. Defaults to value of AWS_ACCESS_KEY_ID environment variable.')

parser.add_argument('-s', '--secret-key', default=environ.get('AWS_SECRET_ACCESS_KEY', None),
                    help='Optional AWS secret key name. Defaults to value of AWS_SECRET_ACCESS_KEY environment variable.')

parser.add_argument('-l', '--logfile', help='Optional log file name.')

def main():
    args = parser.parse_args()
    
    jobs.setup_logger(args.logfile)
    s3 = S3(args.access_key, args.secret_key, args.bucketname)
    
    #
    # Do the work
    #
    run_name = '{:.3f}'.format(time())
    process(s3, paths.sources, run_name)
    
    #
    # Talk about the work
    #
    render.render(paths.sources, 960, 2, 'render-{}.png'.format(run_name))
    render_data = open('render-{}.png'.format(run_name)).read()
    render_key = s3.new_key(join('runs', run_name, 'render.png'))
    render_key.set_contents_from_string(render_data, policy='public-read',
                                        headers={'Content-Type': 'image/png'})

    summary_html = summarize.summarize(s3)
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
        getLogger('openaddr').debug('Found state in {}'.format(state_key.name))

        state_file = BytesIO(state_key.get_contents_as_string())
        rows = DictReader(state_file, dialect='excel-tab')
        
        for row in rows:
            key = join(sourcedir, row['source'])
            states[key] = dict(cache=row['cache'],
                               version=row['version'],
                               fingerprint=row['fingerprint'],
                               cache_time=row['cache time'],
                               process_time=row['process time'])
    
    return states

def process(s3, sourcedir, run_name):
    '''
    '''
    # Find existing cache information
    source_extras = read_state(s3, sourcedir)
    getLogger('openaddr').info('Loaded {} sources from state.txt'.format(len(source_extras)))
    
    # Cache data, if necessary
    source_files = glob(join(sourcedir, '*.json'))
    source_files.sort(key=lambda s: source_extras[s]['cache_time'], reverse=True)
    
    results = jobs.run_all_process2s(source_files, source_extras)
    states = collect_states([path for path in results.values() if path])
    upload_states(s3, states, run_name)

def collect_states(result_paths):
    ''' Read a list of process2.process() result paths, collect into one list.
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
            if state[key]:
                state[key] = join(dirname(result_path), state[key])
        
        states.append([state[key] for key in columns])
    
    return states

def upload_states(s3, states, run_name):
    '''
    '''
    columns = states[0]
    new_states = states[:1]
    
    for values in states[1:]:
        state = dict(zip(columns, values))
        source, _ = splitext(state['source'])
        getLogger('openaddr').debug('Uploading files for {}'.format(source))
        
        if state['cache']:
            #
            # TODO: follow this YYYYMMDD date path pattern for cached data:
            # http://s3.amazonaws.com/data.openaddresses.io/20141204/us-wa-king.zip
            #
            _, cache_ext = splitext(state['cache'])
            key_name = '/{}/{}{}'.format(run_name, source, cache_ext)
            key = s3.new_key(key_name)

            kwargs = dict(policy='public-read', reduced_redundancy=True)
            key.set_contents_from_filename(state['cache'], **kwargs)

            state['cache'] = key.generate_url(expires_in=0, query_auth=False)
            state['fingerprint'] = key.md5
            state['version'] = run_name
    
        if state['sample']:
            #
            # TODO: follow this YYYYMMDD date path pattern for sample data:
            # http://s3.amazonaws.com/data.openaddresses.io/20141226/samples/za-wc-cape_town.json
            #
            _, sample_ext = splitext(state['sample'])
            key_name = '/{}/{}{}'.format(run_name, source, sample_ext)
            key = s3.new_key(key_name)

            kwargs = dict(policy='public-read', reduced_redundancy=True)
            key.set_contents_from_filename(state['sample'], **kwargs)

            state['sample'] = key.generate_url(expires_in=0, query_auth=False)
    
        if state['processed']:
            # http://s3.amazonaws.com/data.openaddresses.io/us-tx-denton.csv
            _, processed_ext = splitext(state['processed'])
            key_name = '/{}/{}{}'.format(run_name, source, processed_ext)
            key = s3.new_key(key_name)

            kwargs = dict(policy='public-read', reduced_redundancy=True)
            key.set_contents_from_filename(state['processed'], **kwargs)

            state['processed'] = key.generate_url(expires_in=0, query_auth=False)
    
        if state['output']:
            # us-tx-denton.txt
            _, output_ext = splitext(state['output'])
            key_name = '/{}/{}{}'.format(run_name, source, output_ext)
            key = s3.new_key(key_name)

            kwargs = dict(policy='public-read', reduced_redundancy=True)
            key.set_contents_from_filename(state['output'], **kwargs)

            state['output'] = key.generate_url(expires_in=0, query_auth=False)
        
        new_states.append([state[col] for col in columns])
    
    state_file = BytesIO()
    out = writer(state_file, dialect='excel-tab')
    
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
    
    getLogger('openaddr').info('Wrote {} sources to state'.format(len(new_states) - 1))
    
    return new_states

if __name__ == '__main__':
    exit(main())
