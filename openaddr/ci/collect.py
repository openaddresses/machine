import logging; _L = logging.getLogger('openaddr.ci.collect')

from argparse import ArgumentParser
from os import close, remove, utime, environ
from zipfile import ZipFile, ZIP_DEFLATED
from os.path import relpath, splitext, exists
from urlparse import urlparse
from tempfile import mkstemp
from calendar import timegm
import urllib

from dateutil.parser import parse

from .objects import read_latest_set, read_completed_runs_to_date
from . import db_connect, db_cursor, setup_logger
from .. import S3

parser = ArgumentParser(description='Run some source files.')

parser.add_argument('-o', '--owner', default='openaddresses',
                    help='Github repository owner. Defaults to "openaddresses".')

parser.add_argument('-r', '--repository', default='openaddresses',
                    help='Github repository name. Defaults to "openaddresses".')

parser.add_argument('-d', '--database-url', default=environ.get('DATABASE_URL', None),
                    help='Optional connection string for database. Defaults to value of DATABASE_URL environment variable.')

def main():
    ''' Single threaded worker to serve the job queue.
    '''
    args = parser.parse_args()
    setup_logger(environ.get('AWS_SNS_ARN'))

    # Rely on boto AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY variables.
    s3 = S3(None, None, environ.get('AWS_S3_BUCKET', 'data.openaddresses.io'))
    
    with db_connect(args.database_url) as conn:
        with db_cursor(conn) as db:
            set = read_latest_set(db, args.owner, args.repository)
            runs = read_completed_runs_to_date(db, set.id)
    
    runs = [runs[i] for i in range(0, len(runs), len(runs) / 5)]
    
    output = ZipFile('everything.zip', 'w', ZIP_DEFLATED)
    
    for (source_base, filename) in iterate_local_processed_files(runs):
        _, ext = splitext(filename)

        if ext == '.csv':
            output.write(filename, source_base + ext)
        
        elif ext == '.zip':
            input = ZipFile(filename, 'r')
            for zipinfo in input.infolist():
                output.writestr(zipinfo, input.read(zipinfo.filename))
            input.close()
    
    output.close()

def iterate_local_processed_files(runs):
    ''' Yield a stream of local processed result files for a list of runs.
    '''
    for run in runs:
        source_base, _ = splitext(relpath(run.source_path, 'sources'))
        processed_url = run.state.get('processed')
    
        print source_base
        print processed_url
        
        if not processed_url:
            continue
        
        try:
            filename = download_processed_file(processed_url)
            yield (source_base, filename)
        
        finally:
            if exists(filename):
                print 'rm', filename
                remove(filename)

def download_processed_file(url):
    ''' Download a URL to a local temporary file, return its path.
    
        Local file will have an appropriate timestamp and extension.
    '''
    _, ext = splitext(urlparse(url).path)
    handle, filename = mkstemp(prefix='processed-', suffix=ext)
    close(handle)
    
    _, head = urllib.urlretrieve(url, filename)
    timestamp = timegm(parse(head.get('Last-Modified')).utctimetuple())
    utime(filename, (timestamp, timestamp))
    
    return filename

if __name__ == '__main__':
    exit(main())
