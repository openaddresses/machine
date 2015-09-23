from __future__ import print_function

from sys import stderr
from zipfile import ZipFile
from os.path import splitext
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
    
    cmd = '/home/migurski/tippecanoe/tippecanoe', '-r', '2', '-l', 'openaddresses', \
          '-X', '-n', 'OpenAddresses YYYY-MM-DD', '-f', '-o', '/tmp/openaddresses.mbtiles'
    
    tippecanoe = Popen(cmd, stdin=PIPE, bufsize=1)
    zip_filenames = (fn for (_, fn, _) in iterate_local_processed_files(runs))
    
    for feature in get_all_features(zip_filenames):
        print(json.dumps(feature), file=tippecanoe.stdin)
    
    tippecanoe.stdin.close()
    tippecanoe.wait()

def get_all_features(zip_filenames):
    ''' Generate a stream of all locations as GeoJSON features.
    '''
    for fn in zip_filenames:
        print(fn, file=stderr)
        zipfile = ZipFile(fn, mode='r')
        for filename in zipfile.namelist():
            _, ext = splitext(filename)
            if ext == '.csv':
                print(filename, file=stderr)
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
                break

if __name__ == '__main__':
    exit(main())
