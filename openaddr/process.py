from argparse import ArgumentParser
from collections import defaultdict
from os.path import join, basename, relpath, splitext
from csv import writer, DictReader
from StringIO import StringIO
from logging import getLogger
from os import environ
from json import dumps
from time import time
from glob import glob

from . import paths, jobs, ConformResult, S3, render, summarize

from . import ExcerptResult

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

        state_file = StringIO(state_key.get_contents_as_string())
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
    source_extras1 = read_state(s3, sourcedir)
    getLogger('openaddr').info('Loaded {} sources from state.txt'.format(len(source_extras1)))

    # Cache data, if necessary
    source_files1 = glob(join(sourcedir, '*.json'))
    source_files1.sort(key=lambda s: source_extras1[s]['cache_time'], reverse=True)
    results1 = jobs.run_all_caches(source_files1, source_extras1, s3)
    
    # Proceed only with sources that have a cache
    source_files2 = [s for s in source_files1 if results1[s].cache]
    source_files2.sort(key=lambda s: source_extras1[s]['process_time'], reverse=True)
    source_extras2 = dict([(s, results1[s].todict()) for s in source_files2])
    results2 = jobs.run_all_conforms(source_files2, source_extras2, s3)

    # ???
    source_files3 = [s for s in source_files2 if results1[s].cache]
    source_extras3 = dict([(s, results1[s].todict()) for s in source_files3])
    results3 = jobs.run_all_excerpts(source_files3, source_extras3, s3)
    
    # Gather all results
    write_state(s3, sourcedir, run_name, source_files1, results1, results2, results3)

def write_state(s3, sourcedir, run_name, source_files1, results1, results2, results3):
    '''
    '''
    state_file = StringIO()
    state_args = dict(policy='public-read', headers={'Content-Type': 'text/plain'})
    json_args = dict(policy='public-read', headers={'Content-Type': 'application/json'})
    
    state_list = [('source', 'cache', 'sample', 'geometry type', 'version',
                   'fingerprint', 'cache time', 'processed', 'process time',
                   'output')]
    
    out = writer(state_file, dialect='excel-tab')
    out.writerow(state_list[-1])
    
    for source in source_files1:
        result1 = results1[source]
        result2 = results2.get(source, ConformResult.empty())
        result3 = results3.get(source, ExcerptResult.empty())

        source_name = relpath(source, sourcedir)
        output_name = '{0}.txt'.format(*splitext(source_name))
    
        state_list.append((source_name, result1.cache, result3.sample_data,
                           result3.geometry_type, result1.version, result1.fingerprint,
                           str(result1.elapsed), result2.processed, str(result2.elapsed),
                           output_name))
        
        out.writerow(state_list[-1])
        
        output_path = join('runs', run_name, output_name)
        output_data = '{}\n\n\n{}'.format(result1.output, result2.output)
        s3.new_key(output_path).set_contents_from_string(output_data, **state_args)
    
    state_data = state_file.getvalue()
    state_path = join('runs', run_name, 'state.txt')

    s3.new_key(state_path).set_contents_from_string(state_data, **state_args)
    s3.new_key('state.txt').set_contents_from_string(state_path, **state_args)
    
    json_data = dumps(state_list, indent=2)
    json_path = join('runs', run_name, 'state.json')
    
    s3.new_key(json_path).set_contents_from_string(json_data, **json_args)
    s3.new_key('state.json').set_contents_from_string(dumps(json_path), **json_args)
    
    getLogger('openaddr').info('Wrote {} sources to state'.format(len(source_files1)))

if __name__ == '__main__':
    exit(main())
