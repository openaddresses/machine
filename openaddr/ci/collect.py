import logging; _L = logging.getLogger('openaddr.ci.collect')

from argparse import ArgumentParser
from collections import defaultdict
from os import environ, stat, close, remove
from zipfile import ZipFile, ZIP_DEFLATED
from os.path import splitext, exists, basename, join, dirname
from urllib.parse import urlparse
from operator import attrgetter
from csv import DictReader, DictWriter
from tempfile import mkstemp, mkdtemp
from itertools import product
from io import TextIOWrapper
from datetime import date
from shutil import rmtree
from math import ceil, floor, sqrt

from .objects import read_latest_set, read_completed_runs_to_date
from . import db_connect, db_cursor, setup_logger, log_function_errors
from .. import S3, iterate_local_processed_files, util
from ..conform import OPENADDR_CSV_SCHEMA
from ..compat import PY2

MULTIPART_CHUNK_SIZE = 5 * 1024 * 1024

parser = ArgumentParser(description='Run some source files.')

parser.add_argument('-o', '--owner', default='openaddresses',
                    help='Github repository owner. Defaults to "openaddresses".')

parser.add_argument('-r', '--repository', default='openaddresses',
                    help='Github repository name. Defaults to "openaddresses".')

parser.add_argument('-b', '--bucket', default=environ.get('AWS_S3_BUCKET', None),
                    help='S3 bucket name. Defaults to value of AWS_S3_BUCKET environment variable.')

parser.add_argument('-d', '--database-url', default=environ.get('DATABASE_URL', None),
                    help='Optional connection string for database. Defaults to value of DATABASE_URL environment variable.')

parser.add_argument('-a', '--access-key', help='Deprecated option provided for backwards compatibility.')
parser.add_argument('-s', '--secret-key', help='Deprecated option provided for backwards compatibility.')

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
    setup_logger(None, None, args.sns_arn, log_level=args.loglevel)
    s3 = S3(None, None, args.bucket)
    db_args = util.prepare_db_kwargs(args.database_url)

    with db_connect(**db_args) as conn:
        with db_cursor(conn) as db:
            set = read_latest_set(db, args.owner, args.repository)
            runs = read_completed_runs_to_date(db, set.id)

    dir = mkdtemp(prefix='collected-')

    # Maps of file suffixes to test functions
    area_tests = {
        'global': (lambda result: True), 'us_northeast': is_us_northeast,
        'us_midwest': is_us_midwest, 'us_south': is_us_south,
        'us_west': is_us_west, 'europe': is_europe, 'asia': is_asia,
        'south_america': is_south_america, 'north_america': is_north_america
        }
    sa_tests = {
        '': (lambda result: result.run_state.share_alike != 'true'),
        'sa': (lambda result: result.run_state.share_alike == 'true')
        }

    collections = prepare_collections(s3, set, dir, area_tests, sa_tests)

    for result in iterate_local_processed_files(runs):
        for (collection, test) in collections:
            if test(result):
                collection.collect(result)

    with db_connect(**db_args) as conn:
        with db_cursor(conn) as db:
            db.execute('UPDATE zips SET is_current = false')
            for (collection, test) in collections:
                collection.publish(db)

    rmtree(dir)

def prepare_collections(s3, set, dir, area_tests, sa_tests):
    '''
    '''
    collections = []
    pairs = product(area_tests.items(), sa_tests.items())

    def _and(test1, test2):
        return lambda result: (test1(result) and test2(result))

    for ((area_id, area_test), (attr_id, sa_test)) in pairs:
        area_suffix = ('-' + area_id).rstrip('-')
        attr_suffix = ('-' + attr_id).rstrip('-')
        new_name = 'openaddr-collected{}{}.zip'.format(area_suffix, attr_suffix)
        new_zip = _prepare_zip(set, join(dir, new_name))
        new_collection = CollectorPublisher(s3, new_zip, area_id, attr_id)
        collections.append((new_collection, _and(area_test, sa_test)))

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
    def __init__(self, s3, collection_zip, collection_id, license_attr):
        self.s3 = s3
        self.zip = collection_zip
        self.results = set()
        self.collection_id = collection_id
        self.license_attr = license_attr

    def collect(self, result):
        ''' Add LocalProcessedResult instance to collection zip.
        '''
        _L.info(u'Adding {} to {}'.format(result.source_base, self.zip.filename))
        add_source_to_zipfile(self.zip, result)
        self.results.add(result)

    def publish(self, db):
        ''' Create new S3 object with zipfile name and upload the collection.
        '''
        license_text = util.summarize_result_licenses(self.results)
        self.zip.writestr('LICENSE.txt', license_text.encode('utf8'))

        self.zip.close()
        _L.info(u'Finished {}'.format(self.zip.filename))

        zip_key = write_to_s3(self.s3.bucket, self.zip.filename, basename(self.zip.filename))
        _L.info(u'Uploaded {} to {}'.format(self.zip.filename, zip_key.name))

        zip_url = util.s3_key_url(zip_key)
        length = stat(self.zip.filename).st_size if exists(self.zip.filename) else None

        db.execute('''DELETE FROM zips WHERE url = %s''', (zip_url, ))

        db.execute('''INSERT INTO zips
                      (url, datetime, is_current, content_length, collection, license_attr)
                      VALUES (%s, NOW(), true, %s, %s, %s)''',
                   (zip_url, length, self.collection_id, self.license_attr))

def _upload_s3_part(s3_bucket, multipart_id, part_num, source_path, offset, bytes, retries=3):
    """ Uploads a part to S3 with retries.
    """
    while True:
        retries -= 1
        try:
            _L.info('Start uploading part #%d ...', part_num)
            for mp in s3_bucket.get_all_multipart_uploads():
                if mp.id == multipart_id:
                    with open(source_path, 'rb') as fp:
                        fp.seek(offset)
                        mp.upload_part_from_file(fp=fp, part_num=part_num, size=bytes)
                    break
        except Exception:
            if retries == 0:
                _L.info('... Failed uploading part #%d', part_num)
                raise
        else:
            _L.info('... Uploaded part #%d', part_num)
            return

def write_to_s3(s3_bucket, filename, keyname, policy='public-read', content_type='application/zip'):
    ''' Writes the file at `filename` to the S3 key `keyname` using
        S3's multipart upload functionality.

        Returns the S3 Key object for the file that was uploaded.
    '''
    mp = s3_bucket.initiate_multipart_upload(keyname, headers={'Content-Type': content_type})

    # With help from https://gist.github.com/fabiant7t/924094
    source_size = stat(filename).st_size
    bytes_per_chunk = max(int(sqrt(MULTIPART_CHUNK_SIZE) * sqrt(source_size)), MULTIPART_CHUNK_SIZE)
    chunk_count = int(ceil(source_size / float(bytes_per_chunk)))

    for i in range(chunk_count):
        offset = i * bytes_per_chunk
        remaining_bytes = source_size - offset
        bytes = min([bytes_per_chunk, remaining_bytes])
        part_num = i + 1
        _upload_s3_part(s3_bucket, mp.id, part_num, filename, offset, bytes)

    if len(mp.get_all_parts()) != chunk_count:
        mp.cancel_upload()
        raise Exception("Error uploading multipart data, expected {} chunks and got {}".format(chunk_count, len(mp.get_all_parts())))

    mp.complete_upload()
    key = s3_bucket.get_key(keyname)
    key.set_acl(policy)
    return key

def add_csv_to_zipfile(zip_out, arc_filename, file):
    ''' Write csv to zipfile.

        File is assumed to be open in binary mode.
    '''
    handle, tmp_filename = mkstemp(suffix='.csv'); close(handle)

    if not PY2:
        file = TextIOWrapper(file, 'utf8')

    size, squares = .1, defaultdict(lambda: 0)

    with open(tmp_filename, 'w') as output:
        in_csv = DictReader(file)
        out_csv = DictWriter(output, OPENADDR_CSV_SCHEMA, dialect='excel')
        out_csv.writerow({col: col for col in OPENADDR_CSV_SCHEMA})

        for row in in_csv:
            try:
                lat, lon = float(row['LAT']), float(row['LON'])
            except ValueError:
                continue
            
            if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                continue

            out_csv.writerow({col: row.get(col) for col in OPENADDR_CSV_SCHEMA})
            key = floor(lat / size) * size, floor(lon / size) * size
            squares[key] += 1

    zip_out.write(tmp_filename, arc_filename)
    remove(tmp_filename)

    _add_spatial_summary_to_zipfile(zip_out, arc_filename, size, squares)

def _add_spatial_summary_to_zipfile(zip_out, arc_filename, size, squares):
    '''
    '''
    assert size in (.1, .2, .5, 1.)
    F = '{:.1f}'

    handle, tmp_filename = mkstemp(suffix='.csv'); close(handle)

    with open(tmp_filename, 'w') as output:
        columns = 'count', 'lon', 'lat', 'area'
        out_csv = DictWriter(output, columns, dialect='excel')
        out_csv.writerow({col: col for col in columns})

        for ((lat, lon), count) in sorted(squares.items()):
            args = [F.format(n) for n in (lon, lat, lon + size, lat + size)]
            area = 'POLYGON(({0} {1},{0} {3},{2} {3},{2} {1},{0} {1}))'.format(*args)
            out_csv.writerow(dict(count=count, lon=F.format(lon), lat=F.format(lat), area=area))

    prefix, _ = splitext(arc_filename)
    support_csvname = join('summary', prefix+'-summary.csv')
    support_vrtname = join('summary', prefix+'-summary.vrt')

    # Write the contents of the summary file.
    zip_out.write(tmp_filename, support_csvname)

    with open(join(dirname(__file__), 'templates', 'source-summary.vrt'), 'rb') as file:
        args = dict(filename=basename(support_csvname))
        args.update(name=splitext(args['filename'])[0])
        vrt_content = file.read().decode('utf8').format(**args).encode('utf8')

    # Write the contents of the summary file VRT.
    zip_out.writestr(support_vrtname, vrt_content)

    remove(tmp_filename)

def add_source_to_zipfile(zip_out, result):
    ''' Add a LocalProcessedResult to zipfile via add_csv_to_zipfile().
    '''
    _, ext = splitext(result.filename)

    if ext == '.csv':
        with open(result.filename) as file:
            add_csv_to_zipfile(zip_out, result.source_base + ext, file)

    elif ext == '.zip':
        with open(result.filename, 'rb') as file:
            zip_in = ZipFile(file, 'r')
            for zipinfo in zip_in.infolist():
                if zipinfo.filename == 'README.txt':
                    # Skip README files when building collection.
                    continue
                elif splitext(zipinfo.filename)[1] == '.csv':
                    zipped_file = zip_in.open(zipinfo.filename)
                    add_csv_to_zipfile(zip_out, zipinfo.filename, zipped_file)
                else:
                    zip_out.writestr(zipinfo, zip_in.read(zipinfo.filename))

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

def is_south_america(result):
    for iso in ('ar', 'bo', 'br', 'cl', 'co', 'ec', 'gf', 'gy', 'pe', 'py', 'sr', 'uy', 've'):
        if _is_country(iso, result):
            return True

    return False

def is_north_america(result):
    for iso in ('ai', 'ag', 'aw', 'bs', 'bb', 'bz', 'bm', 'bq', 'vg', 'ca',
                'ky', 'cr', 'cu', 'cw', 'dm', 'do', 'sv', 'gl', 'gd', 'gp',
                'gt', 'ht', 'hn', 'jm', 'mq', 'mx', 'pm', 'ms', 'cw', 'kn',
                'ni', 'pa', 'pr', 'bq', 'bq', 'sx', 'kn', 'lc', 'pm', 'vc',
                'tt', 'tc', 'vi'):
        if _is_country(iso, result):
            return True

    return False

if __name__ == '__main__':
    exit(main())
