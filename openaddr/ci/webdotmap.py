import apsw
import boto3
import os
import json
from flask import Blueprint, Response, abort, current_app, render_template, url_for

from . import setup_logger
from .webcommon import log_application_errors, flask_log_level
from .webhooks import get_memcache_client

dots = Blueprint('dots', __name__)


# https://stackoverflow.com/questions/56776974/sqlite3-connect-to-a-database-in-cloud-s3
class S3VFS(apsw.VFS):
    def __init__(self, vfsname="s3", basevfs="", cache=None):
        self.vfsname = vfsname
        self.basevfs = basevfs
        self.cache = cache
        apsw.VFS.__init__(self, self.vfsname, self.basevfs)

    def xOpen(self, name, flags):
        return S3VFSFile(self.basevfs, name, flags, self.cache)


class S3VFSFile():
    def __init__(self, inheritfromvfsname, filename, flags, cache):
        self.s3 = boto3.client('s3')
        self.cache = cache
        self.bucket = filename.uri_parameter("bucket")
        self.key = filename.filename().lstrip("/")

    def _cache_key(self, amount, offset):
        return '{bucket}/{key}/{amount}/{offset}'.format(
            bucket=self.bucket,
            key=self.key,
            amount=amount,
            offset=offset,
        )

    def xRead(self, amount, offset):
        data = None
        if self.cache:
            cache_key = self._cache_key(amount, offset)
            data = self.cache.get(cache_key)

        if data is None:
            response = self.s3.get_object(Bucket=self.bucket, Key=self.key, Range='bytes={}-{}'.format(offset, offset + amount))
            data = response['Body'].read()

            if self.cache:
                self.cache.set(cache_key, data)

        return data

    def xFileSize(self):
        length = None
        if self.cache:
            cache_key = '{bucket}/{key}/size'.format(bucket=self.bucket, key=self.key)
            length = self.cache.get(cache_key)

        if length is None:
            response = self.s3.head_object(Bucket=self.bucket, Key=self.key)
            length = response['ContentLength']

            if self.cache:
                self.cache.set(cache_key, length)

        return length

    def xClose(self):
        pass

    def xFileControl(self, op, ptr):
        return False


def get_mbtiles_connection(bucket, key, cache):
    '''
    '''
    s3vfs = S3VFS(cache=cache)
    return apsw.Connection(
        'file:/{key}?bucket={bucket}&immutable=1'.format(bucket=bucket, key=key),
        flags=apsw.SQLITE_OPEN_READONLY | apsw.SQLITE_OPEN_URI,
        vfs=s3vfs.vfsname,
    )


def get_mbtiles_metadata(bucket, key, cache):
    '''
    '''
    if cache:
        cache_key = '{bucket}/{key}/metadata'.format(bucket=bucket, key=key)
        cached = cache.get(cache_key)
        if cached:
            return cached

    connection = get_mbtiles_connection(bucket, key, cache)
    cur = connection.cursor()

    res = cur.execute('''SELECT name, value FROM metadata
                        WHERE name IN ('center', 'json')''')

    data = dict(res.fetchall())
    lon, lat, zoom = map(float, data.get('center', '0,0,0').split(','))

    more = json.loads(data.get('json', '{}'))
    fields = list(more.get('vector_layers', [])[0].get('fields', {}).keys())

    cur.close()

    metadata_tuple = (zoom, lat, lon, fields)
    if cache:
        cache.set(cache_key, metadata_tuple)

    return metadata_tuple


def get_mbtiles_tile(bucket, key, row, col, zoom, cache):
    '''
    '''
    if cache:
        cache_key = '{bucket}/{key}/{zoom}/{row}/{col}'.format(bucket=bucket, key=key, zoom=zoom, row=row, col=col)
        cached = cache.get(cache_key)
        if cached:
            return cached

    connection = get_mbtiles_connection(bucket, key, cache)
    cur = connection.cursor()

    flipped_row = (2**zoom) - 1 - row

    res = cur.execute('''SELECT tile_data FROM tiles
                         WHERE zoom_level=? AND tile_column=? AND tile_row=?''', (zoom, col, flipped_row))

    data = res.fetchone()

    cur.close()

    if cache:
        cache.set(cache_key, data)

    return data


@dots.route('/runs/<int:run_id>/dotmap/index.html')
@log_application_errors
def dotmap_preview(run_id):
    '''
    '''
    if not run_id:
        abort(404)

    try:
        bucket = "data.openaddresses.io"
        key = "runs/{run_id}/slippymap.mbtiles".format(run_id=run_id)
        mc = get_memcache_client(current_app.config)
        zoom, lat, lon, fields = get_mbtiles_metadata(bucket, key, mc)
    except ValueError:
        abort(500)

    return render_template(
        'dotmap-index.html',
        run_id=run_id,
        zoom=zoom,
        lat=lat,
        lon=lon,
        fields=fields,
        scene_url=url_for('dots.get_scene', run_id=run_id)
    )


@dots.route('/runs/<run_id>/dotmap/scene.yaml')
@log_application_errors
def get_scene(run_id):
    if not run_id:
        abort(404)

    tile_args = dict(run_id=run_id, zoom=123, col=456, row=789)
    tile_url = url_for('dots.get_one_tile', **tile_args).replace('123/456/789', '{z}/{x}/{y}')

    return Response(
        render_template('dotmap-scene.yaml', tile_url=tile_url),
        headers={'Content-Type': 'application/x-yaml'},
    )


@dots.route('/runs/<run_id>/dotmap/tiles/<int:zoom>/<int:col>/<int:row>.mvt')
@log_application_errors
def get_one_tile(run_id, zoom, col, row):
    '''
    '''
    if not run_id:
        abort(404)

    bucket = "data.openaddresses.io"
    key = "runs/{run_id}/slippymap.mbtiles".format(run_id=run_id)
    mc = get_memcache_client(current_app.config)
    body = get_mbtiles_tile(bucket, key, row, col, zoom, mc)

    if not body:
        abort(404)

    headers = {
        'Content-Type': 'application/vnd.mapbox-vector-tile',
        'Content-Encoding': 'gzip',
    }

    return Response(body, headers=headers)


def apply_dotmap_blueprint(app):
    '''
    '''
    @dots.after_request
    def cache_everything(response):
        response.cache_control.max_age = 31556952  # 1 year
        response.cache_control.public = True
        return response

    app.register_blueprint(dots)

    @app.before_first_request
    def app_prepare():
        setup_logger(os.environ.get('AWS_SNS_ARN'), None, flask_log_level(app.config))
