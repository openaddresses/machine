from threading import Thread, Lock
from collections import OrderedDict
from multiprocessing import cpu_count
from logging import getLogger, FileHandler, StreamHandler, Formatter, DEBUG
from time import sleep

from . import cache, conform

def run_all_caches(source_files, bucketname='openaddresses-cfa'):
    '''
    '''
    source_queue = source_files[:]
    destination_files = OrderedDict()
    args = Lock(), source_queue, destination_files, bucketname

    threads = [Thread(target=run_cache, args=args)
               for i in range(cpu_count() + 1)]
    
    threads.append(Thread(target=run_timer, args=(source_queue, 15)))

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()
    
    return destination_files

def run_cache(lock, source_files, destination_files, bucketname):
    '''
    '''
    while True:
        with lock:
            if not source_files:
                return
            path = source_files.pop(0)
    
        getLogger('openaddr').info(path)
        csv_path = cache(path, 'out', bucketname)
        
        with lock:
            destination_files[path] = csv_path

def run_all_conforms(source_files, source_extras, bucketname='openaddresses-cfa'):
    '''
    '''
    source_queue = source_files[:]
    destination_files = OrderedDict()
    args = Lock(), source_queue, source_extras, destination_files, bucketname

    threads = [Thread(target=run_conform, args=args)
               for i in range(cpu_count() + 1)]
    
    threads.append(Thread(target=run_timer, args=(source_queue, 15)))

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()
    
    return destination_files

def run_conform(lock, source_files, source_extras, destination_files, bucketname):
    '''
    '''
    while True:
        with lock:
            if not source_files:
                return
            path = source_files.pop(0)
            extras = source_extras[path]
    
        getLogger('openaddr').info(path)
        csv_path = conform(path, 'out', extras, bucketname)
        
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
    format = '%(threadName)11s  {0} %(levelname)06s: %(message)s'
    getLogger('openaddr').setLevel(DEBUG)
    
    handler1 = StreamHandler()
    handler1.setFormatter(Formatter(format.format('%(relativeCreated)10.1f')))
    getLogger('openaddr').addHandler(handler1)

    if logfile:
        handler2 = FileHandler(logfile, mode='w')
        handler2.setFormatter(Formatter(format.format('%(asctime)s')))
        getLogger('openaddr').addHandler(handler2)
