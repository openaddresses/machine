from logging import getLogger, FileHandler, StreamHandler, Formatter, DEBUG
from time import sleep

from . import cache, conform

def run_cache(lock, source_files, destination_files):
    '''
    '''
    while True:
        with lock:
            if not source_files:
                return
            path = source_files.pop(0)
    
        getLogger('openaddr').info(path)
        csv_path = cache(path, 'out')
        
        with lock:
            destination_files[path] = csv_path

def run_conform(lock, source_files, destination_files):
    '''
    '''
    while True:
        with lock:
            if not source_files:
                return
            path = source_files.pop(0)
    
        getLogger('openaddr').info(path)
        csv_path = conform(path, 'out')
        
        with lock:
            destination_files[path] = csv_path

def run_timer(source_files, interval):
    '''
    '''
    sleep(interval)

    while source_files:
        getLogger('openaddr').debug('{0} source files remain'.format(len(source_files)))
        sleep(interval)

def setup_logger(logfile):
    ''' Set up logging stream and optional file for 'openaddr' logger.
    '''
    format = '%(threadName)10s  {0} %(levelname)06s: %(message)s'
    getLogger('openaddr').setLevel(DEBUG)
    
    handler1 = StreamHandler()
    handler1.setFormatter(Formatter(format.format('%(relativeCreated)10.1f')))
    getLogger('openaddr').addHandler(handler1)

    if logfile:
        handler2 = FileHandler(logfile, mode='w')
        handler2.setFormatter(Formatter(format.format('%(asctime)s')))
        getLogger('openaddr').addHandler(handler2)
