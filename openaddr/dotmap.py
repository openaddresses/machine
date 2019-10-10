from __future__ import print_function, division
import logging; _L = logging.getLogger('openaddr.dotmap')

from sys import stderr
from datetime import date
from zipfile import ZipFile
from itertools import product
from os.path import splitext, basename
from argparse import ArgumentParser
from urllib.parse import urlparse, parse_qsl, urljoin
from tempfile import mkstemp, gettempdir
from os import environ, close, remove
from shutil import copyfile
from time import sleep
from io import TextIOWrapper
import json, subprocess, csv, sqlite3

from uritemplate import expand
import requests, boto3

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

def call_tippecanoe(mbtiles_filename, include_properties=True):
    '''
    '''
    base_zoom = 15

    cmd = (
        'tippecanoe',
        '--drop-rate', '2',
        '--layer', 'openaddresses',
        '--name', 'OpenAddresses {}'.format(str(date.today())),
        '--force',
        '--drop-densest-as-needed',
        '--temporary-directory', gettempdir(),
        '--output', mbtiles_filename,
    )

    if include_properties:
        full_cmd = cmd + (
            '--include', 'NUMBER', '--include', 'STREET', '--include', 'UNIT',
            '--maximum-zoom', str(base_zoom), '--minimum-zoom', str(base_zoom)
            )
    else:
        full_cmd = cmd + (
            '--exclude-all', '--maximum-zoom', str(base_zoom - 1), '--base-zoom', str(base_zoom)
            )

    _L.info('Running tippcanoe: {}'.format(' '.join(full_cmd)))

    return subprocess.Popen(full_cmd, stdin=subprocess.PIPE, bufsize=1)

def join_tilesets(out_filename, in1_filename, in2_filename):
    '''
    '''
    cmd = 'tile-join', '-f', '-o', out_filename, in1_filename, in2_filename

    _L.info('Running tile-join: {}'.format(' '.join(cmd)))

    proc = subprocess.Popen(cmd, bufsize=1)
    proc.wait()

    if proc.returncode != 0:
        raise RuntimeError('Tile-join command returned {}'.format(proc.returncode))

def split_tilesets(name_prefix, all_hi, all_lo, nw_hi, nw_lo, nw_out, ne_hi, ne_lo, ne_out, se_hi, se_lo, se_out, sw_hi, sw_lo, sw_out):
    '''
    '''
    quadrants = [
        ('northwest', nw_hi, nw_lo, nw_out, '-122.2707,37.8044,13'), # Oakland
        ('northeast', ne_hi, ne_lo, ne_out, '139.7731,35.6793,13'),  # Tokyo
        ('southeast', se_hi, se_lo, se_out, '151.2073,-33.8686,13'), # Sydney
        ('southwest', sw_hi, sw_lo, sw_out, '-56.1975,-34.9057,13'), # Montevideo
        ]

    zooms_cutoffs = [(zoom + 1, 2**zoom) for zoom in range(15)]

    for (quadrant, quad_hi, quad_lo, quad_out, center) in quadrants:
        _L.info('Preparing {} quadrant...'.format(quadrant))
        copyfile(all_hi, quad_hi)
        copyfile(all_lo, quad_lo)

        for filename in (quad_hi, quad_lo):
            with sqlite3.connect(filename) as db:
                for (zoom, cutoff) in zooms_cutoffs:
                    if 'north' in quadrant:
                        db.execute('delete from tiles where zoom_level = ? and tile_row < ?', (zoom, cutoff))
                    if 'south' in quadrant:
                        db.execute('delete from tiles where zoom_level = ? and tile_row >= ?', (zoom, cutoff))
                    if 'east' in quadrant:
                        db.execute('delete from tiles where zoom_level = ? and tile_column < ?', (zoom, cutoff))
                    if 'west' in quadrant:
                        db.execute('delete from tiles where zoom_level = ? and tile_column >= ?', (zoom, cutoff))

        join_tilesets(quad_out, quad_hi, quad_lo)
        remove(quad_hi)
        remove(quad_lo)

        with sqlite3.connect(quad_out) as db:
            tileset_name = '{} {}'.format(name_prefix.capitalize(), quadrant.capitalize()).lstrip()
            db.execute("update metadata set value = ? where name = 'center'", (center, ))
            db.execute("update metadata set value = ? where name in ('name', 'description')",
                       ('OpenAddresses {} {}'.format(str(date.today()), tileset_name), ))

            if quadrant == 'northwest':
                db.execute("update metadata set value = ? where name = 'bounds'", ('-180,0,0,85.05', ))
            if quadrant == 'northeast':
                db.execute("update metadata set value = ? where name = 'bounds'", ('0,0,180,85.05', ))
            if quadrant == 'southeast':
                db.execute("update metadata set value = ? where name = 'bounds'", ('0,-85.05,180,0', ))
            if quadrant == 'southwest':
                db.execute("update metadata set value = ? where name = 'bounds'", ('-180,-85.05,0,0', ))

        yield quadrant, quad_out

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
    session = boto3.session.Session(access_id, secret_key, session_token)
    s3 = session.resource('s3')
    bucket = s3.Bucket(bucket)

    _L.debug('{} --> {}'.format(mbtiles_path, s3_key))
    bucket.upload_file(mbtiles_path, s3_key)

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
        status = requests.get(api_url).json()

        if status.get('error') is not None:
            raise RuntimeError(str(status['error']))
        elif status.get('complete') is True:
            break

        sleep(30)

parser = ArgumentParser(description='Make a dot map.')

parser.add_argument('-t', '--tileset-id', help='Deprecated option kept for backward-compatibility.')

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

parser.add_argument('-n', '--name-prefix', default='',
                    help='Optional Mapbox tileset name prefix.')

parser.add_argument('--sns-arn', default=environ.get('AWS_SNS_ARN', None),
                    help='Optional AWS Simple Notification Service (SNS) resource. Defaults to value of AWS_SNS_ARN environment variable.')

def main():
    args = parser.parse_args()
    setup_logger(args.sns_arn, None)

    _L.info("Fetching runs from database...")
    with connect_db(args.database_url) as conn:
        with db_cursor(conn) as db:
            set = read_latest_set(db, args.owner, args.repository)
            runs = read_completed_runs_to_date(db, set.id)
            _L.info("Using set %s with %d runs.", set.id, len(runs))

    mbtiles_filenames = list()

    # Prepare 14 temporary files for use in preparing and cutting MBTiles output.
    for i in range(14):
        handle, mbtiles_filename = mkstemp(prefix='oa-{:02d}-'.format(i), suffix='.mbtiles')
        _L.info("Added %s as temp mbtiles filename", mbtiles_filename)
        mbtiles_filenames.append(mbtiles_filename)
        close(handle)

    # Stream all features to two tilesets: high-zoom and low-zoom.
    _L.info("Instantiating tippecanoes")
    tippecanoe_hi = call_tippecanoe(mbtiles_filenames[0], True)
    tippecanoe_lo = call_tippecanoe(mbtiles_filenames[1], False)
    results = iterate_local_processed_files(runs)

    _L.info("Streaming all features")
    for feature in stream_all_features(results):
        line = json.dumps(feature).encode('utf8') + b'\n'
        tippecanoe_hi.stdin.write(line)
        tippecanoe_lo.stdin.write(line)

    _L.info("Finished streaming features")
    tippecanoe_hi.stdin.close()
    tippecanoe_lo.stdin.close()
    tippecanoe_hi.wait()
    tippecanoe_lo.wait()

    status_hi, status_lo = tippecanoe_hi.returncode, tippecanoe_lo.returncode
    _L.info("Tippecanoes are finished. Highzoom status: %s, Lowzoom status: %s", status_hi, status_lo)

    if status_hi != 0 and status_lo != 0:
        raise RuntimeError('High- and low-zoom Tippecanoe commands returned {} and {}'.format(status_hi, status_lo))
    elif status_hi != 0:
        raise RuntimeError('High-zoom Tippecanoe command returned {}'.format(status_hi))
    elif status_lo != 0:
        raise RuntimeError('Low-zoom Tippecanoe command returned {}'.format(status_lo))

    # Split world tilesets into quadrants and upload them to Mapbox.
    for (quadrant, mbtiles_filename) in split_tilesets(args.name_prefix, *mbtiles_filenames):
        tileset_name = '{}-{}'.format(args.name_prefix, quadrant).lstrip('-')
        tileset_id = '{}.{}'.format(args.mapbox_user, tileset_name)
        mapbox_upload(mbtiles_filename, tileset_id, args.mapbox_user, args.mapbox_key)
        _L.info("Uploaded %s to mapbox", mbtiles_filename)

    _L.info("Done updating dotmap.")

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
                fileobj = zipfile.open(filename)
                buffer = TextIOWrapper(fileobj, encoding='utf8')
                for row in csv.DictReader(buffer):
                    try:
                        lon_lat = float(row['LON']), float(row['LAT'])
                        properties = {k: v for (k, v) in row.items() if k not in ('LON', 'LAT')}
                        feature = {"type": "Feature", "properties": properties,
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
