import logging; _L = logging.getLogger('openaddr.ci.collect')

from ..compat import standard_library

from argparse import ArgumentParser
from os import environ
from zipfile import ZipFile, ZIP_DEFLATED
from os.path import splitext, exists, basename, join
from urllib.parse import urlparse
from operator import attrgetter
from tempfile import mkstemp, mkdtemp
from itertools import product
from datetime import date
from shutil import rmtree

from .objects import read_latest_set, read_completed_runs_to_date
from . import db_connect, db_cursor, setup_logger, render_index_maps, log_function_errors
from .. import S3, iterate_local_processed_files

parser = ArgumentParser(description='Run some source files.')

parser.add_argument('-o', '--owner', default='openaddresses',
                    help='Github repository owner. Defaults to "openaddresses".')

parser.add_argument('-r', '--repository', default='openaddresses',
                    help='Github repository name. Defaults to "openaddresses".')

parser.add_argument('-b', '--bucket', default='data.openaddresses.io',
                    help='S3 bucket name. Defaults to "data.openaddresses.io".')

parser.add_argument('-d', '--database-url', default=environ.get('DATABASE_URL', None),
                    help='Optional connection string for database. Defaults to value of DATABASE_URL environment variable.')

parser.add_argument('-a', '--access-key', default=environ.get('AWS_ACCESS_KEY_ID', None),
                    help='Optional AWS access key name. Defaults to value of AWS_ACCESS_KEY_ID environment variable.')

parser.add_argument('-s', '--secret-key', default=environ.get('AWS_SECRET_ACCESS_KEY', None),
                    help='Optional AWS secret key name. Defaults to value of AWS_SECRET_ACCESS_KEY environment variable.')

parser.add_argument('--sns-arn', default=environ.get('AWS_SNS_ARN', None),
                    help='Optional AWS Simple Notification Service (SNS) resource. Defaults to value of AWS_SNS_ARN environment variable.')

parser.add_argument('-v', '--verbose', help='Turn on verbose logging',
                    action='store_const', dest='loglevel',
                    const=logging.DEBUG, default=logging.INFO)

parser.add_argument('-q', '--quiet', help='Turn off most logging',
                    action='store_const', dest='loglevel',
                    const=logging.WARNING, default=logging.INFO)

@log_function_errors
def main():
    ''' Single threaded worker to serve the job queue.
    '''
    args = parser.parse_args()
    setup_logger(args.sns_arn, log_level=args.loglevel)
    s3 = S3(args.access_key, args.secret_key, args.bucket)
    
    with db_connect(args.database_url) as conn:
        with db_cursor(conn) as db:
            set = read_latest_set(db, args.owner, args.repository)
            runs = read_completed_runs_to_date(db, set.id)

    render_index_maps(s3, runs)
    
    dir = mkdtemp(prefix='collected-')
    
    # Maps of file suffixes to test functions
    area_tests = {
        '-global': (lambda result: True), '-us_northeast': is_us_northeast,
        '-us_midwest': is_us_midwest, '-us_south': is_us_south, 
        '-us_west': is_us_west, '-europe': is_europe, '-asia': is_asia
        }
    attr_tests = {
        '-by': (lambda result: result.run_state.get('attribution required', '') != 'false'),
        '': (lambda result: result.run_state.get('attribution required', '') == 'false')
        }
    
    collections = prepare_collections(s3, set, dir, area_tests, attr_tests)

    for result in iterate_local_processed_files(runs):
        for (collection, test) in collections:
            if test(result):
                collection.collect(result)
    
    for (collection, test) in collections:
        collection.publish()
    
    rmtree(dir)

def prepare_collections(s3, set, dir, area_tests, attr_tests):
    '''
    '''
    collections = []
    pairs = product(area_tests.items(), attr_tests.items())
    
    def _and(test1, test2):
        return lambda result: (test1(result) and test2(result))

    for ((area_suffix, area_test), (attr_suffix, attr_test)) in pairs:
        new_name = 'openaddr-collected{}{}.zip'.format(area_suffix, attr_suffix)
        new_zip = _prepare_zip(set, join(dir, new_name))
        new_collection = CollectorPublisher(s3, new_zip)
        collections.append((new_collection, _and(area_test, attr_test)))
        
    return collections

def _prepare_zip(set, filename):
    '''
    '''
    zipfile = ZipFile(filename, 'w', ZIP_DEFLATED, allowZip64=True)
    
    sources_tpl = 'https://github.com/{owner}/{repository}/tree/{commit_sha}/sources'
    sources_url = sources_tpl.format(**set.__dict__)
    zipfile.writestr('README.txt', '''Data collected around {date} by OpenAddresses (http://openaddresses.io).

Address data is essential infrastructure. Street names, house numbers and
postal codes, when combined with geographic coordinates, are the hub that
connects digital to physical places.

Data licenses can be found in LICENSE.txt.

Data source information can be found at
{url}
'''.format(url=sources_url, date=date.today()))
    
    return zipfile

class CollectorPublisher:
    ''' 
    '''
    def __init__(self, s3, collection_zip):
        self.s3 = s3
        self.zip = collection_zip
        self.sources = dict()
    
    def collect(self, result):
        ''' Add LocalProcessedResult instance to collection zip.
        '''
        _L.info(u'Adding {} to {}'.format(result.source_base, self.zip.filename))
        add_source_to_zipfile(self.zip, result.source_base, result.filename)

        attribution = 'No'
        if result.run_state.get('attribution flag') != 'false':
            attribution = result.run_state.get('attribution name')
    
        self.sources[result.source_base] = {
            'website': result.run_state.get('website') or 'Unknown',
            'license': result.run_state.get('license') or 'Unknown',
            'attribution': attribution
            }
        
    def publish(self):
        ''' Create new S3 object with zipfile name and upload the collection.
        '''
        # Write a short file with source licenses.
        template = u'{source}\nWebsite: {website}\nLicense: {license}\nRequired attribution: {attribution}\n'
        license_lines = [u'Data collected by OpenAddresses (http://openaddresses.io).\n']
        for (source, data) in sorted(self.sources.items()):
            line = template.format(source=source, **data)
            license_lines.append(line)

        self.zip.writestr('LICENSE.txt', u'\n'.join(license_lines).encode('utf8'))

        self.zip.close()
        _L.info(u'Finished {}'.format(self.zip.filename))

        zip_key = self.s3.new_key(basename(self.zip.filename))
        zip_args = dict(policy='public-read', headers={'Content-Type': 'application/zip'})
        zip_key.set_contents_from_filename(self.zip.filename, **zip_args)
        _L.info(u'Uploaded {} to {}'.format(self.zip.filename, zip_key.name))

def add_source_to_zipfile(zip_out, source_base, filename):
    '''
    '''
    _, ext = splitext(filename)

    if ext == '.csv':
        zip_out.write(filename, source_base + ext)
    
    elif ext == '.zip':
        zip_in = ZipFile(filename, 'r')
        for zipinfo in zip_in.infolist():
            if zipinfo.filename == 'README.txt':
                # Skip README files when building collection.
                continue
            zip_out.writestr(zipinfo, zip_in.read(zipinfo.filename))
        zip_in.close()

def _is_us_state(abbr, result):
    for sep in ('/', '-'):
        if result.source_base == 'us{sep}{abbr}'.format(**locals()):
            return True

        if result.source_base.startswith('us{sep}{abbr}.'.format(**locals())):
            return True

        if result.source_base.startswith('us{sep}{abbr}{sep}'.format(**locals())):
            return True

    return False

def is_us_northeast(result):
    for abbr in ('ct', 'me', 'ma', 'nh', 'ri', 'vt', 'nj', 'ny', 'pa'):
        if _is_us_state(abbr, result):
            return True

    return False
    
def is_us_midwest(result):
    for abbr in ('il', 'in', 'mi', 'oh', 'wi', 'ia', 'ks', 'mn', 'mo', 'ne', 'nd', 'sd'):
        if _is_us_state(abbr, result):
            return True

    return False
    
def is_us_south(result):
    for abbr in ('de', 'fl', 'ga', 'md', 'nc', 'sc', 'va', 'dc', 'wv', 'al',
                 'ky', 'ms', 'ar', 'la', 'ok', 'tx', 'tn'):
        if _is_us_state(abbr, result):
            return True

    return False
    
def is_us_west(result):
    for abbr in ('az', 'co', 'id', 'mt', 'nv', 'nm', 'ut', 'wy', 'ak', 'ca', 'hi', 'or', 'wa'):
        if _is_us_state(abbr, result):
            return True

    return False
    
def _is_country(iso, result):
    for sep in ('/', '-'):
        if result.source_base == iso:
            return True

        if result.source_base.startswith('{iso}.'.format(**locals())):
            return True

        if result.source_base.startswith('{iso}{sep}'.format(**locals())):
            return True

    return False

def is_europe(result):
    for iso in ('be', 'bg', 'cz', 'dk', 'de', 'ee', 'ie', 'el', 'es', 'fr',
                'hr', 'it', 'cy', 'lv', 'lt', 'lu', 'hu', 'mt', 'nl', 'at',
                'pl', 'pt', 'ro', 'si', 'sk', 'fi', 'se', 'uk', 'gr', 'gb'  ):
        if _is_country(iso, result):
            return True

    return False
    
def is_asia(result):
    for iso in ('af', 'am', 'az', 'bh', 'bd', 'bt', 'bn', 'kh', 'cn', 'cx',
                'cc', 'io', 'ge', 'hk', 'in', 'id', 'ir', 'iq', 'il', 'jp',
                'jo', 'kz', 'kp', 'kr', 'kw', 'kg', 'la', 'lb', 'mo', 'my',
                'mv', 'mn', 'mm', 'np', 'om', 'pk', 'ph', 'qa', 'sa', 'sg',
                'lk', 'sy', 'tw', 'tj', 'th', 'tr', 'tm', 'ae', 'uz', 'vn',
                'ye', 'ps',
                
                'as', 'au', 'nz', 'ck', 'fj', 'pf', 'gu', 'ki', 'mp', 'mh',
                'fm', 'um', 'nr', 'nc', 'nz', 'nu', 'nf', 'pw', 'pg', 'mp',
                'sb', 'tk', 'to', 'tv', 'vu', 'um', 'wf', 'ws', 'is'):
        if _is_country(iso, result):
            return True

    return False

if __name__ == '__main__':
    exit(main())
