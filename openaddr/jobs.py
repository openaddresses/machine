'''Run many conform jobs in parallel.
   The basic implementation is Python's multiprocessing.Pool

   Signals used:
   SIGUSR1: send to master process to shut down all work and proceed to reporting
   SIGUSR2: send to master process to print stack trace and drop to debugger
   SIGALRM: send to a worker to time it out. (we set an alarm on each worker)
   SIGTERM: not explicitly handled. killing a worker causes weird failures, avoid.
'''

import logging; _L = logging.getLogger('openaddr.jobs')

from collections import OrderedDict
import multiprocessing
import signal
import traceback
import time
import os
import os.path
import json
import setproctitle

from . import process_one, compat

#
# Configuration variables
#

# After this many seconds, a job will be killed with SIGALRM
global_job_timeout = 2 * 60 * 60

# Seconds between job queue status updates
report_interval = 60

# Number of jobs to run at once
thread_count = multiprocessing.cpu_count() * 2

# Global variables used to manage killing the pool
pool = None
abort_requested = False

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

def abort_pool(signum, frame):
    '''Signal handler, last-ditch effort to salvage a run if something's spinning
    '''
    global pool, abort_requested
    _L.error("Received SIGUSR1, initiate abort sequence.")
    abort_requested = True
    if pool:
        _L.error("Terminating pool...")
        pool.terminate()
        _L.error("...Pool terminated waiting for processes to exit...")
        pool.join()
        _L.error("...processes exited. All jobs aborted.")
        _L.error("Process should exit in < %d seconds", report_interval)

def invoke_debugger(signum, frame):
    '''Signal handler to print a stack trace and drop into debugger on the controlling terminal.
    '''
    _L.error("=== Debugger invoked ===")
    _L.error("=== Stack trace of parent process ===\n %s", ''.join(traceback.format_stack(frame)))
    import pdb; pdb.set_trace()

def run_all_process_ones(source_files, destination, source_extras):
    ''' Run process_one.process() for all source files in parallel, return a collection of results.
    '''
    global pool, abort_requested

    # Make sure our destination directory exists
    try:
        os.mkdir(destination)
    except OSError:
        pass

    # Set up a signal handler to terminate the pool. Last ditch abort without losing all work.
    signal.signal(signal.SIGUSR1, abort_pool)
    signal.signal(signal.SIGUSR2, invoke_debugger)

    # Create task objects
    tasks = tuple(Task(source_path, destination, source_extras.get(source_path, {})) for source_path in source_files)
    _L.info("%d tasks created", len(tasks))

    # Result collection object
    results = OrderedDict()
    result_count = 0

    # Set up a pool to run our jobs. (don't use maxtasksperchild, causes problems.)
    pool = multiprocessing.Pool(processes=thread_count)

    # Start the tasks. Results can arrive out of order.
    _L.info("Running tasks in pool with %d processes", thread_count)
    result_iter = pool.imap_unordered(_run_task, tasks, chunksize = 1)
    _L.info("You can terminate the jobs with kill -USR1 %d", os.getpid())

    # Iterate through the results as they come
    while not abort_requested:
        try:
            # Block for N seconds waiting for the next result to come to us
            completed_path, result = result_iter.next(timeout=report_interval)
            _L.info("Result received for %s", completed_path)
            results[completed_path] = result
            result_count += 1
        except multiprocessing.TimeoutError:
            # Not an error; just the timeout from next() letting us check in on queue status
            pass
        except StopIteration:
            # The whole queue is done, so go ahead and exit
            _L.info("All jobs complete, closing the pool.")
            pool.close()
            _L.info("Joining to the pool.")
            pool.join()
            return results
        except KeyboardInterrupt:      # What about SystemExit?
            # User hit Ctrl-C; just propagate this up so Python aborts
            raise
        except:
            # Some other exception; should almost never occur, usually process() catches errors
            _L.error("Task threw an exception that process() didn't catch", exc_info=True)
            result_count += 1
            # Swallow the error so that the whole pool doesn't die.
        finally:
            _L.info("Job completion: %d/%d = %d%%", result_count, len(tasks), (100*result_count/len(tasks)))

    if abort_requested:
        _L.warning("Job abort requested, bailing out of conform jobs.")
        return results

    _L.error("This function should never reach this point.")
    raise Exception("Job queue exited in an odd manner, the run is probably broken.")


class Task(object):
    '''A single task of work to do.
       Has no application-specific logic, just stores application-specific state
       and invokes the application's method.
    '''
    def __init__(self, source_path, destination, extras):
        self.source_path = source_path
        self.destination = destination
        self.extras = extras

    @timeout(global_job_timeout)
    def run(self):
        start = time.time()
        _L.info("Starting task for %s with PID %d", self.source_path, os.getpid())
        setproctitle.setproctitle("openaddr %s" % os.path.basename(self.source_path))
        result = process_one.process(self.source_path, self.destination, self.extras)
        _L.info("Finished task in %ds for %s", (time.time()-start), self.source_path)
        setproctitle.setproctitle("openaddr idle")
        return self.source_path, result

def _run_task(task):
    'Shim to invoke class method'
    return task.run()

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
