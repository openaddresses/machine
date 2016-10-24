import logging; _L = logging.getLogger('openaddr.ci.tileindex')

from ..compat import standard_library

from io import TextIOWrapper
from operator import attrgetter
from tempfile import mkstemp, mkdtemp
from zipfile import ZipFile, ZIP_DEFLATED
from itertools import groupby, zip_longest
from os.path import splitext, join, exists
from os import close, environ, mkdir
from argparse import ArgumentParser
from random import randint
from csv import DictReader

from . import db_connect, db_cursor, setup_logger, log_function_errors, collect
from .objects import read_latest_set, read_completed_runs_to_date
from .. import S3, iterate_local_processed_files, util
from ..conform import OPENADDR_CSV_SCHEMA
from ..compat import gzopen, csvDictWriter

BLOCK_SIZE = 100000
SOURCE_COLNAME = 'OA:Source'
TILE_SIZE = 1.

class Point:
    
    def __init__(self, lon, lat, result, row):
        self.row = row
        self.result = result
        self.key = int(lon // TILE_SIZE), int(lat // TILE_SIZE) # Southwest corner lon, lat

class Tile:

    columns = OPENADDR_CSV_SCHEMA + [SOURCE_COLNAME]

    def __init__(self, key, dirname):
        self.key = key
        self.dirname = dirname
        self.results = set()
        
        handle, self.filename = mkstemp(prefix='tile-', suffix='.csv.gz', dir=dirname)
        close(handle)
        
        with gzopen(self.filename, 'wt', encoding='utf8') as file:
            rows = csvDictWriter(file, Tile.columns, encoding='utf8')
            rows.writerow({k: k for k in Tile.columns})
    
    def add_points(self, points):
        with gzopen(self.filename, 'at', encoding='utf8') as file:
            rows = csvDictWriter(file, Tile.columns, encoding='utf8')
            for point in points:
                self.results.add(point.result)

                row = {SOURCE_COLNAME: point.result.source_base}
                row.update(point.row)
                rows.writerow(row)
    
    def publish(self, s3_bucket):
        '''
        '''
        handle, zip_filename = mkstemp(prefix='tile-', suffix='.zip', dir=self.dirname)
        close(handle)
        
        zipfile = ZipFile(zip_filename, 'w', ZIP_DEFLATED, allowZip64=True)
        
        with gzopen(self.filename, 'rb') as file:
            zipfile.writestr('addresses.csv', file.read())

        zipfile.close()
        keyname = 'tiles/{:.1f}/{:.1f}.zip'.format(*self.key)
        
        collect.write_to_s3(s3_bucket, zipfile.filename, keyname)

parser = ArgumentParser(description='Create a tiled spatial index of CSV data in S3.')

parser.add_argument('-o', '--owner', default='openaddresses',
                    help='Github repository owner. Defaults to "openaddresses".')

parser.add_argument('-r', '--repository', default='openaddresses',
                    help='Github repository name. Defaults to "openaddresses".')

parser.add_argument('-b', '--bucket', default=environ.get('AWS_S3_BUCKET', None),
                    help='S3 bucket name. Defaults to value of AWS_S3_BUCKET environment variable.')

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
    setup_logger(args.access_key, args.secret_key, args.sns_arn, log_level=args.loglevel)
    s3 = S3(args.access_key, args.secret_key, args.bucket)
    db_args = util.prepare_db_kwargs(args.database_url)

    with db_connect(**db_args) as conn:
        with db_cursor(conn) as db:
            set = read_latest_set(db, args.owner, args.repository)
            runs = read_completed_runs_to_date(db, set and set.id)

    print([r.source_path for r in runs])
    dir = mkdtemp(prefix='tileindex-')
    
    print(dir)
    addresses = iterate_runs_points(runs)
    point_blocks = iterate_point_blocks(addresses)
    tiles = populate_tiles(dir, point_blocks)
    
    for tile in tiles.values():
        print(tile.key, '-', len(tile.results), 'sources')
        tile.publish(s3.bucket)

def iterate_runs_points(runs):
    ''' Iterate over all the points.
    '''
    for result in iterate_local_processed_files(runs, sort_on='source_path'):
        _L.info('Indexing points from {}'.format(result.source_base))
        print(result.run_state.processed)
        _L.debug('filename: {}'.format(result.filename))
        _L.debug('run_state: {}'.format(result.run_state))
        _L.debug('code_version: {}'.format(result.code_version))
        with open(result.filename, 'rb') as file:
            result_zip = ZipFile(file)
            
            csv_infos = [zipinfo for zipinfo in result_zip.infolist()
                         if splitext(zipinfo.filename)[1] == '.csv']
            
            if not csv_infos:
                break

            zipped_file = result_zip.open(csv_infos[0].filename)
            point_rows = DictReader(TextIOWrapper(zipped_file))
            
            for row in point_rows:
                try:
                    lat, lon = float(row['LAT']), float(row['LON'])
                except ValueError:
                    # Skip this point if the lat/lon don't parse
                    continue
                
                # Include this point if it's on Earth
                if -180 <= lon <= 180 and -90 <= lat <= 90:
                    yield Point(lon, lat, result, row)

def iterate_point_blocks(points):
    ''' Group points into blocks by key, generate (key, points) pairs.
    '''
    args, filler = [points] * BLOCK_SIZE, Point(0, -99, None, None) # Illegal lon, lat
    
    for block in zip_longest(*args, fillvalue=filler):
        point_block = sorted(block, key=attrgetter('key'))
        
        for key, key_points in groupby(point_block, attrgetter('key')):
            if key is not filler.key:
                key_points_list = list(key_points)
                _L.debug('Found {} points in tile {}'.format(len(key_points_list), key))
                yield (key, key_points_list)
    
    _L.debug('{} remain'.format(len(list(points))))

def populate_tiles(dirname, point_blocks):
    ''' Return a dictionary of Tiles keyed on southwest lon, lat.
    '''
    tiles = dict()
    
    for (key, points) in point_blocks:
        if key not in tiles:
            tile_dirname = join(dirname, str(randint(100, 999)))
            if not exists(tile_dirname):
                mkdir(tile_dirname)
            _L.debug('Adding Tile: {}'.format(key))
            tiles[key] = Tile(key, tile_dirname)
        
        tiles[key].add_points(points)
    
    return tiles

if __name__ == '__main__':
    exit(main())
