import logging; _L = logging.getLogger('openaddr.ci.tileindex')

from ..compat import standard_library

from zipfile import ZipFile
from os.path import splitext
from tempfile import mkstemp
from io import TextIOWrapper
from operator import attrgetter
from itertools import groupby, zip_longest
from csv import DictReader
from os import close

from .. import iterate_local_processed_files
from ..conform import OPENADDR_CSV_SCHEMA
from ..compat import csvopen, csvDictWriter

BLOCK_SIZE = 10000
SOURCE_COLNAME = 'OA:Source'

class Point:
    
    def __init__(self, lon, lat, source_base, row):
        self.lon = lon
        self.lat = lat
        self.row = row
        self.source_base = source_base
        
        self.key = int(lon // 1.), int(lat // 1.) # Southwest corner lon, lat

class Tile:

    columns = OPENADDR_CSV_SCHEMA + [SOURCE_COLNAME]

    def __init__(self, key, dirname):
        self.key = key
        
        handle, self.filename = mkstemp(prefix='tile-', suffix='.csv', dir=dirname)
        close(handle)
        
        with csvopen(self.filename, 'w', 'utf8') as file:
            rows = csvDictWriter(file, Tile.columns, encoding='utf8')
            rows.writerow({k: k for k in Tile.columns})
    
    def add_points(self, points):
        with csvopen(self.filename, 'a', 'utf8') as file:
            rows = csvDictWriter(file, Tile.columns, encoding='utf8')
            for point in points:
                row = {SOURCE_COLNAME: point.source_base}
                row.update(point.row)
                rows.writerow(row)

def iterate_runs_points(runs):
    ''' Iterate over all the points.
    '''
    for result in iterate_local_processed_files(runs):
        _L.debug('source_base:', result.source_base)
        _L.debug('filename:', result.filename)
        _L.debug('run_state:', result.run_state)
        _L.debug('code_version:', result.code_version)
        with open(result.filename, 'rb') as file:
            result_zip = ZipFile(file)
            
            csv_infos = [zipinfo for zipinfo in result_zip.infolist()
                         if splitext(zipinfo.filename)[1] == '.csv']
            
            if not csv_infos:
                break

            zipped_file = result_zip.open(csv_infos[0].filename)
            point_rows = DictReader(TextIOWrapper(zipped_file))
            
            for row in point_rows:
                lat, lon = float(row['LAT']), float(row['LON'])
                yield Point(lon, lat, result.source_base, row)

def iterate_point_blocks(points):
    ''' Group points into blocks by key, generate (key, points) pairs.
    '''
    args, filler = [points] * BLOCK_SIZE, Point(0, -99, None, None) # Illegal lon, lat
    
    for block in zip_longest(*args, fillvalue=filler):
        point_block = sorted(block, key=attrgetter('key'))
        
        for key, key_points in groupby(point_block, attrgetter('key')):
            if key is not filler.key:
                _L.debug('key:', key)
                yield (key, key_points)
    
    _L.debug(len(list(points)), 'remain')

def populate_tiles(dirname, point_blocks):
    '''
    '''
    tiles = dict()
    
    for (key, points) in point_blocks:
        if key not in tiles:
            print('Adding', key)
            tiles[key] = Tile(key, dirname)
        
        tiles[key].add_points(points)
    
    return tiles
