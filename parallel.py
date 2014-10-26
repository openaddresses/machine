from argparse import ArgumentParser
from os.path import join
from glob import glob

from openaddr import paths, jobs

parser = ArgumentParser(description='Run some source files.')
parser.add_argument('-l', '--logfile', help='Optional log file name.')

if __name__ == '__main__':

    args = parser.parse_args()
    jobs.setup_logger(args.logfile)

    source_files1 = glob(join(paths.sources, '*.json'))
    results1 = jobs.run_all_caches(source_files1, 'openaddresses-cfa')
    print results1
    
    # Proceed only with sources that have a cache
    source_files2 = [s for s in source_files1 if results1[s]['cache']]
    source_extras = dict([(s, results1[s]) for s in source_files2])
    results2 = jobs.run_all_conforms(source_files2, source_extras, 'openaddresses-cfa')
    print results2
