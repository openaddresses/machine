from __future__ import print_function, division
import logging; _L = logging.getLogger('openaddr.dotmap')

from .compat import standard_library

from sys import stderr
from zipfile import ZipFile
from os.path import splitext, basename
from subprocess import Popen, PIPE
from argparse import ArgumentParser
from urllib.parse import urlparse, parse_qsl, urljoin
from tempfile import mkstemp, gettempdir
from os import environ, close
from time import sleep
import json

from uritemplate import expand
import requests, boto3

from .compat import csvIO, csvDictReader
from .ci import db_connect, db_cursor, setup_logger
from .ci.objects import read_latest_set, read_completed_runs_to_date
from . import iterate_local_processed_files

MAPBOX_API_BASE = 'https://api.mapbox.com/uploads/v1/'

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

def mapbox_upload(mbtiles_path, tileset, username, api_key):
    ''' Upload MBTiles file to a tileset on Mapbox API.
    
        https://github.com/openaddresses/openaddresses.io/blob/gh-pages/_src/make_mbtiles.sh
    '''
    _L.info("Uploading {} to Mapbox {}'s {}".format(basename(mbtiles_path), username, tileset))
    
    session_token, access_id, secret_key, bucket, s3_key, url \
        = _mapbox_get_credentials(username, api_key)

    _upload_to_s3(mbtiles_path, session_token, access_id, secret_key, bucket, s3_key)
    _mapbox_create_upload(url, tileset, username, api_key)

def _mapbox_get_credentials(username, api_key):
    ''' Get a tuple of Mapbox API upload credentials.
    
        Returns sessionToken, accessKeyId, secretAccessKey, bucket, key, url.
    
        https://www.mapbox.com/developers/api/uploads/#Stage.a.file.on.Amazon.S3
    '''
    template = urljoin(MAPBOX_API_BASE, '{username}/credentials{?access_token}')
    api_url = expand(template, dict(username=username, access_token=api_key))
    _L.debug('GET {}'.format(urlparse(api_url).path))
    got = requests.get(api_url)
    
    if got.status_code not in range(200, 299):
        raise Exception('Not {}'.format(got.status_code))
    
    resp = got.json()
    
    return (resp['sessionToken'], resp['accessKeyId'], resp['secretAccessKey'],
            resp['bucket'], resp['key'], resp['url'])

def _upload_to_s3(mbtiles_path, session_token, access_id, secret_key, bucket, s3_key):
    ''' Upload MBTiles file to S3 using Mapbox credentials.
    
        https://www.mapbox.com/developers/api/uploads/#Stage.a.file.on.Amazon.S3
    '''
    old_AWS_SESSION_TOKEN = environ.get('AWS_SESSION_TOKEN')
    old_AWS_ACCESS_KEY_ID = environ.get('AWS_ACCESS_KEY_ID')
    old_AWS_SECRET_ACCESS_KEY = environ.get('AWS_SECRET_ACCESS_KEY')
    
    environ['AWS_SESSION_TOKEN'] = session_token
    environ['AWS_ACCESS_KEY_ID'] = access_id
    environ['AWS_SECRET_ACCESS_KEY'] = secret_key

    try:
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket)

        _L.debug('{} --> {}'.format(mbtiles_path, s3_key))
        bucket.upload_file(mbtiles_path, s3_key)
    
    finally:
        del environ['AWS_SESSION_TOKEN']
        del environ['AWS_ACCESS_KEY_ID']
        del environ['AWS_SECRET_ACCESS_KEY']

        if old_AWS_SESSION_TOKEN is not None:
            environ['AWS_SESSION_TOKEN'] = AWS_SESSION_TOKEN

        if old_AWS_ACCESS_KEY_ID is not None:
            environ['AWS_ACCESS_KEY_ID'] = AWS_ACCESS_KEY_ID

        if old_AWS_SECRET_ACCESS_KEY is not None:
            environ['AWS_SECRET_ACCESS_KEY'] = AWS_SECRET_ACCESS_KEY

def _mapbox_create_upload(url, tileset, username, api_key):
    ''' Create Mapbox upload for credentials and S3 URL, wait for completion.
        
        https://www.mapbox.com/developers/api/uploads/#Create.a.new.upload
    '''
    template = urljoin(MAPBOX_API_BASE, '{username}{?access_token}')
    api_url = expand(template, dict(username=username, access_token=api_key))
    data = json.dumps(dict(tileset=tileset, url=url))
    _L.debug('POST {} {}'.format(urlparse(api_url).path, repr(data)))

    posted = requests.post(api_url, data=data, headers={'Content-Type': 'application/json'})
    
    if posted.status_code not in range(200, 299):
        raise Exception('Not {}'.format(posted.status_code))
    
    resp = posted.json()
    return _mapbox_wait_for_upload(resp['id'], username, api_key)

def _mapbox_wait_for_upload(id, username, api_key):
    ''' Wait for upload completion.
        
        https://www.mapbox.com/developers/api/uploads/#Retrieve.state.of.an.upload
    '''

    template = urljoin(MAPBOX_API_BASE, '{username}/{id}{?access_token}')
    api_url = expand(template, dict(username=username, id=id, access_token=api_key))

    while True:
        _L.debug('GET {}'.format(urlparse(api_url).path))
        got = requests.get(api_url)

        if got.json().get('complete') is True:
            break
        
        sleep(3)

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
    
    cmd = 'tippecanoe', '-r', '2', '-l', 'openaddresses', \
          '-X', '-n', 'OpenAddresses YYYY-MM-DD', '-f', \
          '-t', gettempdir(), '-o', mbtiles_filename
    
    _L.info('Running tippcanoe: {}'.format(' '.join(cmd)))
    
    tippecanoe = Popen(cmd, stdin=PIPE, bufsize=1)
    zip_details = ((source, filename) for (source, filename, _)
                   in iterate_local_processed_files(runs))
    
    for feature in stream_all_features(zip_details):
        print(json.dumps(feature), file=tippecanoe.stdin)
    
    tippecanoe.stdin.close()
    tippecanoe.wait()
    
    mapbox_upload(mbtiles_filename, args.tileset_id, args.mapbox_user, args.mapbox_key)

def stream_all_features(zip_details):
    ''' Generate a stream of all locations as GeoJSON features.
    '''
    for (source_base, zip_filename) in zip_details:
        _L.debug(u'Opening {} ({})'.format(zip_filename, source_base))

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
