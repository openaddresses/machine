from __future__ import print_function, division

from sys import stderr
from zipfile import ZipFile
from os.path import splitext
from tempfile import gettempdir
from subprocess import Popen, PIPE
import json

from .compat import csvIO, csvDictReader
from .ci import db_connect, db_cursor
from .ci.objects import read_latest_set, read_completed_runs_to_date
from . import iterate_local_processed_files

def main():
    with db_connect('postgres://xxx:xxxx@machine-db.openaddresses.io:5432/xxx?sslmode=require') as conn:
        with db_cursor(conn) as db:
            set = read_latest_set(db, 'openaddresses', 'hooked-on-sources')
            runs = read_completed_runs_to_date(db, set.id)
    
    cmd = '/home/migurski/tippecanoe/tippecanoe', '-r', '2', \
          '-l', 'openaddresses', '-X', '-n', 'OpenAddresses YYYY-MM-DD', '-f', \
          '-t', gettempdir(), '-o', '/tmp/openaddresses.mbtiles'
    
    tippecanoe = Popen(cmd, stdin=PIPE, bufsize=1)
    zip_filenames = (fn for (_, fn, _) in iterate_local_processed_files(runs))
    
    for feature in stream_all_features(zip_filenames):
        print(json.dumps(feature), file=tippecanoe.stdin)
    
    tippecanoe.stdin.close()
    tippecanoe.wait()

def stream_all_features(zip_filenames):
    ''' Generate a stream of all locations as GeoJSON features.
    '''
    for zip_filename in zip_filenames:
        zipfile = ZipFile(zip_filename, mode='r')
        for filename in zipfile.namelist():
            # Look for the one expected .csv file in the zip archive.
            _, ext = splitext(filename)
            if ext == '.csv':
                # Yield GeoJSON point objects with no properties.
                buffer = csvIO(zipfile.read(filename))
                for row in csvDictReader(buffer, encoding='utf8'):
                    try:
                        lon_lat = float(row['LON']), float(row['LAT'])
                        feature = {"type": "Feature", "properties": {}, 
                            "geometry": {"type": "Point", "coordinates": lon_lat}}
                    except ValueError:
                        pass
                    else:
                        yield feature

                # Move on to the next zip archive.
                zipfile.close()
                break

if __name__ == '__main__':
    exit(main())
