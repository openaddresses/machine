from threading import Thread, Lock
from collections import OrderedDict
from multiprocessing import cpu_count
from logging import getLogger, FileHandler, StreamHandler, Formatter, DEBUG
from time import sleep

from . import cache, conform

def run_all_caches(source_files, source_extras, bucketname='openaddresses-cfa'):
    ''' Run cache() for all source files in parallel, return a dict of results.
    '''
    source_queue, results = source_files[:], OrderedDict()
    args = Lock(), source_queue, source_extras, results, bucketname

    threads = [Thread(target=_run_cache, args=args)
               for i in range(cpu_count() + 1)]
    
    threads.append(Thread(target=_run_timer, args=(source_queue, 15)))

    _wait_for_threads(threads, source_queue)
    
    return results

def _run_cache(lock, source_queue, source_extras, results, bucketname):
    ''' Single queue worker for source files to conform().
    
        Keep going until source_queue is empty.
    '''
    while True:
        with lock:
            if not source_queue:
                return
            path = source_queue.pop(0)
            extras = source_extras.get(path, dict())
    
        try:
            getLogger('openaddr').info(path)
            result = cache(path, 'out', extras, bucketname)
        except:
            result = dict(cache=None, fingerprint=None, version=None)
        
        with lock:
            results[path] = result

def run_all_conforms(source_files, source_extras, bucketname='openaddresses-cfa'):
    ''' Run conform() for all source files in parallel, return a dict of results.
    '''
    source_queue, results = source_files[:], OrderedDict()
    args = Lock(), source_queue, source_extras, results, bucketname

    threads = [Thread(target=_run_conform, args=args)
               for i in range(cpu_count() + 1)]
    
    threads.append(Thread(target=_run_timer, args=(source_queue, 15)))

    _wait_for_threads(threads, source_queue)
    
    return results

def _run_conform(lock, source_queue, source_extras, results, bucketname):
    ''' Single queue worker for source files to conform().
    
        Keep going until source_queue is empty.
    '''
    while True:
        with lock:
            if not source_queue:
                return
            path = source_queue.pop(0)
            extras = source_extras.get(path, dict())
    
        try:
            getLogger('openaddr').info(path)
            result = conform(path, 'out', extras, bucketname)
        except:
            result = dict(processed=None, path=None)
        
        with lock:
            results[path] = result

def _run_timer(source_queue, interval):
    ''' Natter on and on about how much of the queue is left.
    '''
    sleep(interval)

    while source_queue:
        getLogger('openaddr').debug('{0} source files remain'.format(len(source_queue)))
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

def _wait_for_threads(threads, queue):
    ''' Run all the threads and wait for them, but catch interrupts.
        
        If a keyboard interrupt is caught, empty the queue and let threads finish.
    '''
    try:
        # Start all the threads.
        for thread in threads:
            thread.start()

        # Check with each thread once per second, to stay interruptible.
        while True:
            for thread in threads:
                thread.join(1)
    except (KeyboardInterrupt, SystemExit):
        getLogger('openaddr').info('Cancel with {0} tasks left'.format(len(queue)))
        while queue:
            # Empty the queue to stop the threads.
            source_queue.pop()
        raise
