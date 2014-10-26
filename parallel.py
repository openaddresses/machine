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

    results1 = jobs.run_all_caches(source_files, 'openaddresses-cfa')
    print results1
    
    results2 = jobs.run_all_conforms(source_files, 'openaddresses-cfa')
    print results2
