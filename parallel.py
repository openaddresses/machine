from argparse import ArgumentParser
from os.path import join, basename, relpath
from csv import writer, DictReader
from StringIO import StringIO
from logging import getLogger
from os import environ
from glob import glob

from openaddr import paths, jobs, ConformResult, S3

parser = ArgumentParser(description='Run some source files.')

parser.add_argument('bucketname',
                    help='Required S3 bucket name.')

parser.add_argument('-a', '--access-key', default=environ['AWS_ACCESS_KEY_ID'],
                    help='Optional AWS access key name. Defaults to value of AWS_ACCESS_KEY_ID environment variable.')

parser.add_argument('-s', '--secret-key', default=environ['AWS_SECRET_ACCESS_KEY'],
                    help='Optional AWS secret key name. Defaults to value of AWS_SECRET_ACCESS_KEY environment variable.')

parser.add_argument('-l', '--logfile', help='Optional log file name.')

if __name__ == '__main__':

    args = parser.parse_args()
    jobs.setup_logger(args.logfile)
    
    s3 = S3(args.access_key, args.secret_key, args.bucketname)
    bucket = s3.connection.get_bucket(s3.bucketname)
    
    # Find existing cache information
    state_key = bucket.get_key('state.txt')
    source_extras1 = dict()

    if state_key:
        state_file = StringIO(state_key.get_contents_as_string())
        rows = DictReader(state_file, dialect='excel-tab')
        
        for row in rows:
            key = join(paths.sources, row['source'])
            source_extras1[key] = dict(cache=row['cache'],
                                       version=row['version'],
                                       fingerprint=row['fingerprint'])
    
    getLogger('openaddr').info('Loaded {} sources from state.txt'.format(len(source_extras1)))

    # Cache data, if necessary
    source_files1 = glob(join(paths.sources, '*.json'))
    results1 = jobs.run_all_caches(source_files1, source_extras1, s3)
    
    # Proceed only with sources that have a cache
    source_files2 = [s for s in source_files1 if results1[s].cache]
    source_extras2 = dict([(s, results1[s].todict()) for s in source_files2])
    results2 = jobs.run_all_conforms(source_files2, source_extras2, s3)

    # Gather all results
    state_file = StringIO()
    out = writer(state_file, dialect='excel-tab')
    
    out.writerow(('source', 'cache', 'version', 'fingerprint', 'cache time', 'processed', 'process time'))
    
    for source in source_files1:
        result1 = results1[source]
        result2 = results2.get(source, ConformResult.empty())
    
        out.writerow((relpath(source, paths.sources), result1.cache,
                      result1.version, result1.fingerprint, result1.elapsed,
                      result2.processed, result2.elapsed))
    
    state_data = state_file.getvalue()
    state_args = dict(policy='public-read', headers={'Content-Type': 'text/plain'})
    bucket.new_key('state.txt').set_contents_from_string(state_data, **state_args)
    
    getLogger('openaddr').info('Wrote {} sources to state.txt'.format(len(source_files1)))
