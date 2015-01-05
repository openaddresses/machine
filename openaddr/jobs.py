from threading import Thread, Lock
from collections import OrderedDict
from multiprocessing import cpu_count
import logging
from logging import getLogger
from os.path import isdir
from time import sleep
from os import mkdir

from . import process_one

def _run_timer(source_queue, interval):
    ''' Natter on and on about how much of the queue is left.
    '''
    sleep(interval)

    while source_queue:
        getLogger('openaddr').debug('{0} source files remain'.format(len(source_queue)))
        sleep(interval)

def run_all_process_ones(source_files, destination, source_extras):
    ''' Run process_one.process() for all source files in parallel, return ???.
    '''
    source_queue, results = source_files[:], OrderedDict()
    args = Lock(), source_queue, destination, source_extras, results
    thread_count = min(cpu_count() * 2, len(source_files))

    threads = [Thread(target=_run_process_one, args=args)
               for i in range(thread_count)]
    
    if len(source_files) > thread_count:
        threads.append(Thread(target=_run_timer, args=(source_queue, 15)))

    _wait_for_threads(threads, source_queue)
    
    return results

def _run_process_one(lock, source_queue, destination, source_extras, results):
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
            if not isdir(destination):
                try:
                    mkdir(destination)
                except OSError:
                    pass
        
            getLogger('openaddr').info(path)
            result = process_one.process(path, destination, extras)
        except:
            getLogger('openaddr').error('Error while running process_one.process', exc_info=True)
            result = None
        
        with lock:
            results[path] = result

def setup_logger(logfile = None, log_level = logging.DEBUG, log_stderr = True):
    ''' Set up logging for openaddr code.
        Default configuration is to log warning and above to stderr
        If logfile is set, logging stream and optional file for 'openaddr' logger.
    '''
    # Get a handle for the openaddr logger and its children
    openaddr_logger = logging.getLogger('openaddr')

    # Default logging format. {0} will be replaced with a destination-appropriate timestamp
    log_format = '%(threadName)11s  {0} %(module)12s:%(lineno)-4d %(levelname)06s: %(message)s'

    # Set the default log level as requested
    openaddr_logger.setLevel(log_level)

    # Remove all previously installed handlers
    for old_handler in openaddr_logger.handlers:
        openaddr_logger.removeHandler(old_handler)
    
    # Set up a logger to stderr
    if log_stderr:
        handler1 = logging.StreamHandler()
        handler1.setFormatter(logging.Formatter(log_format.format('%(relativeCreated)10.1f')))
        openaddr_logger.addHandler(handler1)

    # Set up a logger to a file
    if logfile:
        handler2 = logging.FileHandler(logfile, mode='w')
        handler2.setFormatter(logging.Formatter(log_format.format('%(asctime)s')))
        openaddr_logger.addHandler(handler2)

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
