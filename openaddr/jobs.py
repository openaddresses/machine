'''Run many conform jobs in parallel.
   The basic implementation is Python's multiprocessing.Pool

   Signals used:
   SIGUSR1: send to master process to shut down all work and proceed to reporting
   SIGUSR2: send to master process to print stack trace and drop to debugger
   SIGALRM: send to a worker to time it out. (we set an alarm on each worker)
   SIGTERM: not explicitly handled. killing a worker causes weird failures, avoid.
'''

import logging; _L = logging.getLogger('openaddr.jobs')

from datetime import timedelta
import multiprocessing
import signal
import traceback
import time
import os
import os.path
import json

from . import process_one, compat

#
# Configuration variables
#

# After this long, a job will be killed with SIGALRM
JOB_TIMEOUT = timedelta(hours=9)

class JobTimeoutException(Exception):
    ''' Exception raised if a per-job timeout fires.
    '''
    def __init__(self, jobstack=[]):
        super(JobTimeoutException, self).__init__()
        self.jobstack = jobstack

# http://stackoverflow.com/questions/8616630/time-out-decorator-on-a-multprocessing-function
def timeout(timeout):
    ''' Function decorator that raises a JobTimeoutException exception
        after timeout seconds, if the decorated function did not return.
    '''

    def decorate(f):
        def timeout_handler(signum, frame):
            raise JobTimeoutException(traceback.format_stack())

        def new_f(*args, **kwargs):
            old_handler = signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(timeout)

            result = f(*args, **kwargs)  # f() always returns, in this scheme

            signal.signal(signal.SIGALRM, old_handler)  # Old signal handler is restored
            signal.alarm(0)  # Alarm removed
            return result

        if compat.PY2:
            new_f.func_name = f.func_name
        else:
            new_f.__name__ = f.__name__

        return new_f

    return decorate

# This code really has nothing to do with jobs, just lives here for lack of a better place.
def setup_logger(logfile = None, log_level = logging.DEBUG, log_stderr = True, log_config_file = "~/.openaddr-logging.json"):
    ''' Set up logging for openaddr code.
        If the file ~/.openaddr-logging.json exists, it will be used as a DictConfig
        Otherwise a default configuration will be set according to function parameters.
        Default is to log DEBUG and above to stderr, and nothing to a file.
    '''
    # Get a handle for the openaddr logger and its children
    openaddr_logger = logging.getLogger('openaddr')

    # Default logging format. {0} will be replaced with a destination-appropriate timestamp
    log_format = '%(process)06s  {0} %(levelname)06s: %(message)s'

    # Set the logger level to show everything, and filter down in the handlers.
    openaddr_logger.setLevel(logging.DEBUG)

    # Remove all previously installed handlers
    for old_handler in openaddr_logger.handlers:
        openaddr_logger.removeHandler(old_handler)

    # Tell multiprocessing to log messages as well. Multiprocessing can interact strangely
    # with logging, see http://bugs.python.org/issue23278
    mp_logger = multiprocessing.get_logger()
    mp_logger.propagate=True

    log_config_file = os.path.expanduser(log_config_file)
    if os.path.exists(log_config_file):
        # Use a JSON config file in the user's home directory if it exists
        # See http://victorlin.me/posts/2012/08/26/good-logging-practice-in-python
        log_config_dict = json.load(file(log_config_file))
        # Override this flag; needs to be set for our module-level loggers to work.
        log_config_dict['disable_existing_loggers'] = False
        logging.config.dictConfig(log_config_dict)
        openaddr_logger.info("Using logger config at %s", log_config_file)
    else:
        # No config file? Set up some sensible defaults

        # Set multiprocessing level as requested
        mp_logger.setLevel(log_level)

        everything_logger = logging.getLogger()
        # Set up a logger to stderr
        if log_stderr:
            handler1 = logging.StreamHandler()
            handler1.setLevel(log_level)
            handler1.setFormatter(logging.Formatter(log_format.format('%(relativeCreated)10.1f')))
            everything_logger.addHandler(handler1)
        # Set up a logger to a file
        if logfile:
            handler2 = logging.FileHandler(logfile, mode='w')
            handler2.setLevel(log_level)
            handler2.setFormatter(logging.Formatter(log_format.format('%(asctime)s')))
            everything_logger.addHandler(handler2)
