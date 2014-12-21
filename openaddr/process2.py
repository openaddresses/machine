from urlparse import urlparse
from os.path import join, basename, dirname, exists, splitext, relpath
from shutil import copy, move
from logging import getLogger
from os import mkdir, rmdir
import tempfile, json, csv

from . import cache, conform, excerpt, ConformResult, ExcerptResult
from .jobs import setup_logger

def process(source, destination):
    '''
    '''
    temp_dir = tempfile.mkdtemp(prefix='process2-')
    temp_src = join(temp_dir, basename(source))
    copy(source, temp_src)
    
    #
    # Cache source data.
    #
    result1 = cache(temp_src, temp_dir, dict())
    
    scheme, _, _, _, _, _ = urlparse(result1.cache)
    if scheme != 'file':
        write_state(source, destination, result1, ConformResult.empty(), ExcerptResult.empty())
        getLogger('openaddr').error('Nothing cached')
        return 1
    
    getLogger('openaddr').info('Cached data in {}'.format(result1.cache))

    #
    # Conform cached source data.
    #
    result2 = conform(temp_src, temp_dir, result1.todict())
    
    if not exists(result2.path):
        write_state(source, destination, result1, result2, ExcerptResult.empty())
        getLogger('openaddr').error('Nothing processed')
        return 1
    
    getLogger('openaddr').info('Processed data in {}'.format(result2.path))
    
    #
    # Excerpt cached source data.
    #
    result3 = excerpt(temp_src, temp_dir, result1.todict())
    
    if not result3.sample_data:
        raise RuntimeError('Nothing excerpted? {}'.format(result3.sample_data))
    
    getLogger('openaddr').info('Sample data in {}'.format(result3.sample_data))
    
    #
    # Write output
    #
    print write_state(source, destination, result1, result2, result3)

def write_state(source, destination, result1, result2, result3):
    '''
    '''
    source_id, _ = splitext(basename(source))
    statedir = join(destination, source_id)
    
    if not exists(statedir):
        mkdir(statedir)
    
    if result1.cache:
        _, _, cache_path1, _, _, _ = urlparse(result1.cache)
        cache_path2 = join(statedir, 'cache{1}'.format(*splitext(cache_path1)))
        copy(cache_path1, cache_path2)

    if result2.path:
        _, _, processed_path1, _, _, _ = urlparse(result2.path)
        processed_path2 = join(statedir, 'out{1}'.format(*splitext(processed_path1)))
        copy(processed_path1, processed_path2)

    if result3.sample_data:
        _, _, sample_path1, _, _, _ = urlparse(result3.sample_data)
        sample_path2 = join(statedir, 'sample{1}'.format(*splitext(sample_path1)))
        copy(sample_path1, sample_path2)
    
    output_path = join(statedir, 'output.txt')

    state = [
        ('source', basename(source)),
        ('cache', result1.cache and relpath(cache_path2, statedir)),
        ('sample', result3.sample_data and relpath(sample_path2, statedir)),
        ('geometry type', result3.geometry_type),
        ('version', result1.version),
        ('fingerprint', result1.fingerprint),
        ('cache time', str(result1.elapsed)),
        ('processed', result2.path and relpath(processed_path2, statedir)),
        ('process time', str(result2.elapsed)),
        ('output', relpath(output_path, statedir))
        ]
    
    with open(output_path, 'w') as file:
        file.write('{}\n\n\n{}'.format(result1.output, result2.output))
               
    with open(join(statedir, 'state.json'), 'w') as file:
        json.dump(zip(*state), file, indent=2)
               
    with open(join(statedir, 'state.txt'), 'w') as file:
        out = csv.writer(file, dialect='excel-tab')
        for row in zip(*state):
            out.writerow(row)
    
        getLogger('openaddr').info('Wrote to state: {}'.format(file.name))
        return file.name

def main():
    '''
    '''
    source = 'tests/sources/us-ca-alameda_county.json'
    destination = 'out'
    
    setup_logger(None)
    
    return process(source, destination)

if __name__ == '__main__':
    exit(main())
