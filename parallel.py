from threading import Thread, Lock
from collections import OrderedDict
from logging import getLogger, FileHandler, StreamHandler, Formatter, DEBUG
from multiprocessing import cpu_count
from argparse import ArgumentParser
from openaddr import conform
from time import sleep

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

parser = ArgumentParser(description='Run some source files.')
parser.add_argument('-l', '--logfile', help='Optional log file name.')

if __name__ == '__main__':

    args = parser.parse_args()
    setup_logger(args.logfile)

    source_files = [
        '/var/opt/openaddresses/sources/us-ca-san_francisco.json',
        '/var/opt/openaddresses/sources/us-ca-alameda_county.json',
        '/var/opt/openaddresses/sources/us-ca-oakland.json',
        '/var/opt/openaddresses/sources/us-ca-berkeley.json'
        ]

    destination_files = OrderedDict()
    args = Lock(), source_files, destination_files

    threads = [Thread(target=run_conform, args=args)
               for i in range(cpu_count() + 1)]
    
    threads.append(Thread(target=run_timer, args=(source_files, 15)))

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()
    
    print destination_files
