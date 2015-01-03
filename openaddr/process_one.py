from __future__ import absolute_import, division, print_function
from future import standard_library; standard_library.install_aliases()

from urllib.parse import urlparse
from os.path import join, basename, dirname, exists, splitext, relpath
from shutil import copy, move, rmtree
from argparse import ArgumentParser
from logging import getLogger
from os import mkdir, rmdir
import tempfile, json, csv

from . import cache, conform, ConformResult

def process(source, destination, extras=dict()):
    ''' Process a single source and destination, return path to JSON state file.
    
        Creates a new directory and files under destination.
    '''
    temp_dir = tempfile.mkdtemp(prefix='process_one-', dir=destination)
    temp_src = join(temp_dir, basename(source))
    copy(source, temp_src)
    
    #
    # Cache source data.
    #
    cache_result = cache(temp_src, temp_dir, extras)
    
    if not cache_result.cache:
        getLogger('openaddr').warning('Nothing cached')
        return write_state(source, destination, cache_result, ConformResult.empty())
    
    getLogger('openaddr').info('Cached data in {}'.format(cache_result.cache))

    #
    # Conform cached source data.
    #
    conform_result = conform(temp_src, temp_dir, cache_result.todict())
    
    if not conform_result.path:
        getLogger('openaddr').warning('Nothing processed')
    else:
        getLogger('openaddr').info('Processed data in {}'.format(conform_result.path))
    
    #
    # Write output
    #
    state_path = write_state(source, destination, cache_result, conform_result, temp_dir)

    rmtree(temp_dir)
    return state_path

def write_state(source, destination, cache_result, conform_result, temp_dir):
    '''
    '''
    source_id, _ = splitext(basename(source))
    statedir = join(destination, source_id)
    
    if not exists(statedir):
        mkdir(statedir)
    
    if cache_result.cache:
        _, _, cache_path1, _, _, _ = urlparse(cache_result.cache)
        cache_path2 = join(statedir, 'cache{1}'.format(*splitext(cache_path1)))
        copy(cache_path1, cache_path2)

    if conform_result.path:
        _, _, processed_path1, _, _, _ = urlparse(conform_result.path)
        processed_path2 = join(statedir, 'out{1}'.format(*splitext(processed_path1)))
        copy(processed_path1, processed_path2)

    # Write the sample data to a sample.json file
    if conform_result.sample:
        sample_path = join(statedir, 'sample.json')
        with open(sample_path, 'w') as sample_file:
            json.dump(conform_result.sample, sample_file, indent=2)
    
    output_path = join(statedir, 'output.txt')

    state = [
        ('source', basename(source)),
        ('cache', cache_result.cache and relpath(cache_path2, statedir)),
        ('sample', conform_result.sample and relpath(sample_path, statedir)),
        ('geometry type', conform_result.geometry_type),
        ('version', cache_result.version),
        ('fingerprint', cache_result.fingerprint),
        ('cache time', cache_result.elapsed and str(cache_result.elapsed)),
        ('processed', conform_result.path and relpath(processed_path2, statedir)),
        ('process time', conform_result.elapsed and str(conform_result.elapsed)),
        ('output', relpath(output_path, statedir))
        ]
    
    with open(output_path, 'w') as file:
        file.write('{}\n\n\n{}'.format(cache_result.output, conform_result.output))
               
    with open(join(statedir, 'index.txt'), 'w') as file:
        out = csv.writer(file, dialect='excel-tab')
        for row in zip(*state):
            out.writerow(row)
    
    with open(join(statedir, 'index.json'), 'w') as file:
        json.dump(zip(*state), file, indent=2)
               
        getLogger('openaddr').info('Wrote to state: {}'.format(file.name))
        return file.name

parser = ArgumentParser(description='Run one source file locally, prints output path.')

parser.add_argument('source', help='Required source file name.')
parser.add_argument('destination', help='Required output directory name.')

parser.add_argument('-l', '--logfile', help='Optional log file name.')

def main():
    '''
    '''
    from .jobs import setup_logger
    args = parser.parse_args()
    setup_logger(args.logfile)

    print(process(args.source, args.destination))

if __name__ == '__main__':
    exit(main())
