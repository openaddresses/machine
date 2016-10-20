import logging; _L = logging.getLogger('openaddr.ci.tileindex')

from ..compat import standard_library

from zipfile import ZipFile
from os.path import splitext
from csv import DictReader
from io import TextIOWrapper
from operator import attrgetter
from itertools import groupby, zip_longest

from .. import iterate_local_processed_files

BLOCK_SIZE = 10000

class Point:
    
    def __init__(self, lon, lat, row):
        self.lon = lon
        self.lat = lat
        self.row = row
        
        self.key = int(lon // 1.), int(lat // 1.) # Southwest corner lon, lat

def iterate_runs_points(runs):
    '''
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
                yield Point(lon, lat, row)

def iterate_point_blocks(points):
    ''' 
    '''
    args, filler = [points] * BLOCK_SIZE, Point(0, -99, None) # Illegal lon, lat
    
    for block in zip_longest(*args, fillvalue=filler):
        point_block = sorted(block, key=attrgetter('key'))
        
        for key, key_points in groupby(point_block, attrgetter('key')):
            if key is not filler.key:
                _L.debug('key:', key)
                yield (key, key_points)
    
    _L.debug(len(list(points)), 'remain')
