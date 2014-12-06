from threading import Thread, Lock
from collections import OrderedDict
from multiprocessing import cpu_count
from logging import getLogger, FileHandler, StreamHandler, Formatter, DEBUG
from os.path import isdir
from time import sleep
from os import mkdir

from . import cache, conform, CacheResult, ConformResult

from . import excerpt, ExcerptResult

def run_all_caches(source_files, source_extras, s3):
    ''' Run cache() for all source files in parallel, return a dict of results.
    '''
    source_queue, results = source_files[:], OrderedDict()
    args = Lock(), source_queue, source_extras, results, s3
    thread_count = min(cpu_count() * 2, len(source_files))

    threads = [Thread(target=_run_cache, args=args)
               for i in range(thread_count)]
    
    if len(source_files) > thread_count:
        threads.append(Thread(target=_run_timer, args=(source_queue, 15)))

    _wait_for_threads(threads, source_queue)
    
    return results

def _run_cache(lock, source_queue, source_extras, results, s3):
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
            if not isdir('out'):
                mkdir('out')
        
            getLogger('openaddr').info(path)
            result = cache(path, 'out', extras, s3)
        except:
            result = CacheResult.empty()
        
        with lock:
            results[path] = result

def run_all_conforms(source_files, source_extras, s3):
    ''' Run conform() for all source files in parallel, return a dict of results.
    '''
    source_queue, results = source_files[:], OrderedDict()
    args = Lock(), source_queue, source_extras, results, s3
    thread_count = min(cpu_count(), len(source_files))

    threads = [Thread(target=_run_conform, args=args)
               for i in range(thread_count)]
    
    if len(source_files) > thread_count:
        threads.append(Thread(target=_run_timer, args=(source_queue, 15)))

    _wait_for_threads(threads, source_queue)
    
    return results

def _run_conform(lock, source_queue, source_extras, results, s3):
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
            if not isdir('out'):
                mkdir('out')
        
            getLogger('openaddr').info(path)
            result = conform(path, 'out', extras, s3)
        except:
            result = ConformResult.empty()
        
        with lock:
            results[path] = result

def run_all_excerpts(source_files, source_extras, s3):
    ''' Run excerpt() for all source files in parallel, return a dict of results.
    '''
    source_queue, results = source_files[:], OrderedDict()
    args = Lock(), source_queue, source_extras, results, s3
    thread_count = min(cpu_count(), len(source_files))

    threads = [Thread(target=_run_excerpt, args=args)
               for i in range(thread_count)]
    
    if len(source_files) > thread_count:
        threads.append(Thread(target=_run_timer, args=(source_queue, 15)))

    _wait_for_threads(threads, source_queue)
    
    return results

def _run_excerpt(lock, source_queue, source_extras, results, s3):
    ''' Single queue worker for source files to excerpt().
    
        Keep going until source_queue is empty.
    '''
    while True:
        with lock:
            if not source_queue:
                return
            path = source_queue.pop(0)
            extras = source_extras.get(path, dict())
    
        try:
            if not isdir('out'):
                mkdir('out')
        
            getLogger('openaddr').info(path)
            result = excerpt(path, 'out', extras, s3)
        except:
            result = ExcerptResult.empty()
        
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
    
    for old_handler in getLogger('openaddr').handlers:
        getLogger('openaddr').removeHandler(old_handler)
    
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
        while threads:
            for thread in threads:
                thread.join(1)
                if not thread.isAlive():
                    # Bury the dead.
                    threads.remove(thread)
                    break

    except (KeyboardInterrupt, SystemExit):
        getLogger('openaddr').info('Cancel with {0} tasks left'.format(len(queue)))
        while queue:
            # Empty the queue to stop the threads.
            queue.pop()
        raise
