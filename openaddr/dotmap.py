from __future__ import print_function, division
import logging; _L = logging.getLogger('openaddr.dotmap')

from .compat import standard_library

from sys import stderr
from datetime import date
from zipfile import ZipFile
from os.path import splitext, basename
from argparse import ArgumentParser
from urllib.parse import urlparse, parse_qsl, urljoin
from tempfile import mkstemp, gettempdir
from os import environ, close
from time import sleep
import json, subprocess

from uritemplate import expand
import requests, boto3

from .compat import csvDictReader, csvIO, PY2
from .ci import db_connect, db_cursor, setup_logger
from .ci.objects import read_latest_set, read_completed_runs_to_date
from . import iterate_local_processed_files

from mapbox import Uploader

def connect_db(dsn):
    ''' Prepare old-style arguments to connect_db().
    '''
    p = urlparse(dsn)
    q = dict(parse_qsl(p.query))
    
    assert p.scheme == 'postgres'
    kwargs = dict(user=p.username, password=p.password, host=p.hostname)
    kwargs.update(dict(database=p.path.lstrip('/')))

    if 'sslmode' in q:
        kwargs.update(dict(sslmode=q['sslmode']))

    return db_connect(**kwargs)

def call_tippecanoe(mbtiles_filename):
    '''
    '''
    cmd = 'tippecanoe', '-r', '2', '-l', 'openaddresses', \
          '-X', '-n', 'OpenAddresses {}'.format(str(date.today())), '-f', \
          '-t', gettempdir(), '-o', mbtiles_filename
    
    _L.info('Running tippcanoe: {}'.format(' '.join(cmd)))
    
    return subprocess.Popen(cmd, stdin=subprocess.PIPE, bufsize=1)

def mapbox_upload(mbtiles_path, tileset, username, api_key):
    ''' Upload MBTiles file to a tileset on Mapbox API.
    
        https://github.com/openaddresses/openaddresses.io/blob/gh-pages/_src/make_mbtiles.sh
    '''
    _L.info("Uploading {} to Mapbox {}'s {}".format(mbtiles_path, username, tileset))

    service = Uploader(access_token=api_key)

    upload_resp = service.upload(mbtiles_path, tileset)
    if upload_resp.status_code == 409:
        for i in range(5):
            sleep(5)
            upload_resp = service.upload(mbtiles_path, tileset)
            if upload_resp.status_code != 409:
                break

    assert upload_resp.status_code == 201

    # # you can wait for the upload to finish processing, but I wouldn't recommend it
    # upload_id = upload_resp.json()['id']
    # _L.info("Waiting for upload to finish processing...")
    # for i in range(60):
    #     status_resp = service.status(upload_id).json()
    #     if status_resp.get('complete'):
    #         break
    #     else:
    #         _L.info("Job status: {}".format(status_resp))
    #     sleep(5)
        


parser = ArgumentParser(description='Make a dot map.')

parser.add_argument('-o', '--owner', default='openaddresses',
                    help='Github repository owner. Defaults to "openaddresses".')

parser.add_argument('-r', '--repository', default='openaddresses',
                    help='Github repository name. Defaults to "openaddresses".')

parser.add_argument('-d', '--database-url', default=environ.get('DATABASE_URL', None),
                    help='Connection string for database. Defaults to value of DATABASE_URL environment variable.')

parser.add_argument('-u', '--mapbox-user', default='open-addresses',
                    help='Mapbox account username. Defaults to "open-addresses".')

parser.add_argument('-m', '--mapbox-key', default=environ.get('MAPBOX_KEY', None),
                    help='Mapbox account key. Defaults to value of MAPBOX_KEY environment variable.')

parser.add_argument('-t', '--tileset-id', default='open-addresses.lec54np1',
                    help='Mapbox tileset ID. Defaults to "open-addresses.lec54np1".')

def main():
    args = parser.parse_args()
    setup_logger(environ.get('AWS_SNS_ARN'))
    
    with connect_db(args.database_url) as conn:
        with db_cursor(conn) as db:
            set = read_latest_set(db, args.owner, args.repository)
            runs = read_completed_runs_to_date(db, set.id)
    
    handle, mbtiles_filename = mkstemp(prefix='oa-', suffix='.mbtiles')
    close(handle)
    
    tippecanoe = call_tippecanoe(mbtiles_filename)
    results = iterate_local_processed_files(runs)
    
    for feature in stream_all_features(results):
        tippecanoe.stdin.write(json.dumps(feature).encode('utf8'))
        tippecanoe.stdin.write(b'\n')
    
    tippecanoe.stdin.close()
    tippecanoe.wait()
    
    mapbox_upload(mbtiles_filename, args.tileset_id, args.mapbox_user, args.mapbox_key)

def stream_all_features(results):
    ''' Generate a stream of all locations as GeoJSON features.
    '''
    for result in results:
        _L.debug(u'Opening {} ({})'.format(result.filename, result.source_base))

        zipfile = ZipFile(result.filename, mode='r')
        for filename in zipfile.namelist():
            # Look for the one expected .csv file in the zip archive.
            _, ext = splitext(filename)
            if ext == '.csv':
                # Yield GeoJSON point objects with no properties.
                bytes = zipfile.read(filename)
                if PY2:
                    buffer = csvIO(bytes)
                else:
                    buffer = csvIO(bytes.decode('utf8'))
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
