from __future__ import absolute_import, division, print_function
import logging; _L = logging.getLogger('openaddr.process_one')

from .compat import standard_library

from urllib.parse import urlparse
from os.path import join, basename, dirname, exists, splitext, relpath
from shutil import copy, move, rmtree
from argparse import ArgumentParser
from os import mkdir, rmdir, close
from _thread import get_ident
import tempfile, json, csv

from . import cache, conform, CacheResult, ConformResult

def process(source, destination, extras=dict()):
    ''' Process a single source and destination, return path to JSON state file.
    
        Creates a new directory and files under destination.
    '''
    temp_dir = tempfile.mkdtemp(prefix='process_one-', dir=destination)
    temp_src = join(temp_dir, basename(source))
    copy(source, temp_src)
    
    log_handler = get_log_handler(temp_dir)
    logging.getLogger('openaddr').addHandler(log_handler)
    
    cache_result, conform_result = CacheResult.empty(), ConformResult.empty()

    try:
        with open(temp_src) as file:
            if json.load(file).get('skip', None):
                raise ValueError('Source says to skip')
    
        # Cache source data.
        cache_result = cache(temp_src, temp_dir, extras)
    
        if not cache_result.cache:
            _L.warning('Nothing cached')
        else:
            _L.info('Cached data in {}'.format(cache_result.cache))

            # Conform cached source data.
            conform_result = conform(temp_src, temp_dir, cache_result.todict())
    
            if not conform_result.path:
                _L.warning('Nothing processed')
            else:
                _L.info('Processed data in {}'.format(conform_result.path))
    
    except Exception:
        _L.warning('Error in process_one.process()', exc_info=True)
    
    finally:
        # Make sure this gets done no matter what
        logging.getLogger('openaddr').removeHandler(log_handler)

    # Write output
    state_path = write_state(source, destination, log_handler,
                             cache_result, conform_result, temp_dir)

    log_handler.close()
    rmtree(temp_dir)
    return state_path

class LogFilter:
    ''' Logging filter object to match only record in the current thread.
    '''
    def __init__(self):
        # Seems to work as unique ID with multiprocessing.Process() as well as threading.Thread()
        self.thread_id = get_ident()
    
    def filter(self, record):
        return record.thread == self.thread_id

def get_log_handler(directory):
    ''' Create a new file handler for the current thread and return it.
    '''
    handle, filename = tempfile.mkstemp(dir=directory, suffix='.log')
    close(handle)
    
    handler = logging.FileHandler(filename)
    handler.setFormatter(logging.Formatter(u'%(asctime)s %(levelname)08s: %(message)s'))
    handler.setLevel(logging.DEBUG)
    handler.addFilter(LogFilter())
    
    return handler

def write_state(source, destination, log_handler, cache_result, conform_result, temp_dir):
    '''
    '''
    source_id, _ = splitext(basename(source))
    statedir = join(destination, source_id)
    
    if not exists(statedir):
        mkdir(statedir)
    
    if cache_result.cache:
        scheme, _, cache_path1, _, _, _ = urlparse(cache_result.cache)
        if scheme in ('file', ''):
            cache_path2 = join(statedir, 'cache{1}'.format(*splitext(cache_path1)))
            copy(cache_path1, cache_path2)
            state_cache = relpath(cache_path2, statedir)
        else:
            state_cache = cache_result.cache
    else:
        state_cache = None

    if conform_result.path:
        _, _, processed_path1, _, _, _ = urlparse(conform_result.path)
        processed_path2 = join(statedir, 'out{1}'.format(*splitext(processed_path1)))
        copy(processed_path1, processed_path2)

    # Write the sample data to a sample.json file
    if conform_result.sample:
        sample_path = join(statedir, 'sample.json')
        with open(sample_path, 'w') as sample_file:
            json.dump(conform_result.sample, sample_file, indent=2)
    
    log_handler.flush()
    output_path = join(statedir, 'output.txt')
    copy(log_handler.stream.name, output_path)

    state = [
        ('source', basename(source)),
        ('cache', state_cache),
        ('sample', conform_result.sample and relpath(sample_path, statedir)),
        ('geometry type', conform_result.geometry_type),
        ('version', cache_result.version),
        ('fingerprint', cache_result.fingerprint),
        ('cache time', cache_result.elapsed and str(cache_result.elapsed)),
        ('processed', conform_result.path and relpath(processed_path2, statedir)),
        ('process time', conform_result.elapsed and str(conform_result.elapsed)),
        ('output', relpath(output_path, statedir))
        ]
               
    with open(join(statedir, 'index.txt'), 'w') as file:
        out = csv.writer(file, dialect='excel-tab')
        for row in zip(*state):
            out.writerow(row)
    
    with open(join(statedir, 'index.json'), 'w') as file:
        json.dump(list(zip(*state)), file, indent=2)
               
        _L.info('Wrote to state: {}'.format(file.name))
        return file.name

parser = ArgumentParser(description='Run one source file locally, prints output path.')

parser.add_argument('source', help='Required source file name.')
parser.add_argument('destination', help='Required output directory name.')

parser.add_argument('-l', '--logfile', help='Optional log file name.')

parser.add_argument('-v', '--verbose', help='Turn on verbose logging',
                    action='store_const', dest='loglevel',
                    const=logging.DEBUG, default=logging.INFO)

parser.add_argument('-q', '--quiet', help='Turn off most logging',
                    action='store_const', dest='loglevel',
                    const=logging.WARNING, default=logging.INFO)

def main():
    '''
    '''
    from .jobs import setup_logger

    args = parser.parse_args()
    setup_logger(logfile=args.logfile, log_level=args.loglevel)
    
    try:
        file_path = process(args.source, args.destination)
    except Exception as e:
        _L.error(e, exc_info=True)
        return 1
    else:
        print(file_path)
        return 0

if __name__ == '__main__':
    exit(main())
