from threading import Thread, Lock
from collections import OrderedDict
from multiprocessing import cpu_count
from argparse import ArgumentParser
from os.path import join
from glob import glob

from openaddr import paths, jobs

parser = ArgumentParser(description='Run some source files.')
parser.add_argument('-l', '--logfile', help='Optional log file name.')

if __name__ == '__main__':

    args = parser.parse_args()
    jobs.setup_logger(args.logfile)

    source_files = glob(join(paths.sources, '*.json'))
    destination_files = OrderedDict()
    args = Lock(), source_files, destination_files

    threads = [Thread(target=jobs.run_conform, args=args)
               for i in range(cpu_count() + 1)]
    
    threads.append(Thread(target=jobs.run_timer, args=(source_files, 15)))

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()
    
    print destination_files
