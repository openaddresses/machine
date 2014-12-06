from subprocess import Popen
from tempfile import mkdtemp
from os.path import realpath, join, basename, splitext, exists, dirname
from shutil import copy, move, rmtree
from mimetypes import guess_extension
from StringIO import StringIO
from logging import getLogger
from datetime import datetime
from os import mkdir, environ
from time import sleep, time
from zipfile import ZipFile
from urlparse import urlparse
from httplib import HTTPConnection
import json

from osgeo import ogr
from requests import get
from boto import connect_s3
from .sample import sample_geojson
from . import paths

geometry_types = {
    ogr.wkbPoint: 'Point',
    ogr.wkbPoint25D: 'Point 2.5D',
    ogr.wkbLineString: 'LineString',
    ogr.wkbLineString25D: 'LineString 2.5D',
    ogr.wkbLinearRing: 'LinearRing',
    ogr.wkbPolygon: 'Polygon',
    ogr.wkbPolygon25D: 'Polygon 2.5D',
    ogr.wkbMultiPoint: 'MultiPoint',
    ogr.wkbMultiPoint25D: 'MultiPoint 2.5D',
    ogr.wkbMultiLineString: 'MultiLineString',
    ogr.wkbMultiLineString25D: 'MultiLineString 2.5D',
    ogr.wkbMultiPolygon: 'MultiPolygon',
    ogr.wkbMultiPolygon25D: 'MultiPolygon 2.5D',
    ogr.wkbGeometryCollection: 'GeometryCollection',
    ogr.wkbGeometryCollection25D: 'GeometryCollection 2.5D',
    ogr.wkbUnknown: 'Unknown'
    }

with open(join(dirname(__file__), 'VERSION')) as file:
    __version__ = file.read().strip()

class CacheResult:
    cache = None
    fingerprint = None
    version = None
    elapsed = None
    output = None
    
    def __init__(self, cache, fingerprint, version, elapsed, output):
        self.cache = cache
        self.fingerprint = fingerprint
        self.version = version
        self.elapsed = elapsed
        self.output = output
    
    @staticmethod
    def empty():
        return CacheResult(None, None, None, None, None)

    def todict(self):
        return dict(cache=self.cache, fingerprint=self.fingerprint, version=self.version)

class ConformResult:
    processed = None
    path = None
    elapsed = None
    output = None

    def __init__(self, processed, path, elapsed, output):
        self.processed = processed
        self.path = path
        self.elapsed = elapsed
        self.output = output
    
    @staticmethod
    def empty():
        return ConformResult(None, None, None, None)

    def todict(self):
        return dict(processed=self.processed, path=self.path)

class ExcerptResult:
    sample_data = None
    geometry_type = None

    def __init__(self, sample_data, geometry_type):
        self.sample_data = sample_data
        self.geometry_type = geometry_type
    
    @staticmethod
    def empty():
        return ExcerptResult(None, None)

    def todict(self):
        return dict(sample_data=self.sample_data)

class S3:
    bucketname = None

    def __init__(self, key, secret, bucketname):
        self._key, self._secret = key, secret
        self.bucketname = bucketname
        self._bucket = connect_s3(key, secret).get_bucket(bucketname)
    
    def toenv(self):
        env = dict(environ)
        env.update(AWS_ACCESS_KEY_ID=self._key, AWS_SECRET_ACCESS_KEY=self._secret)
        return env
    
    def get_key(self, name):
        return self._bucket.get_key(name)
    
    def new_key(self, name):
        return self._bucket.new_key(name)

def cache(srcjson, destdir, extras, s3):
    ''' Python wrapper for openaddress-cache.
    
        Return a dictionary of cache details, including URL and md5 hash:
        
          {
            "cache": URL of cached data,
            "fingerprint": md5 hash of data,
            "version": data version as date?
          }
    '''
    start = datetime.now()
    source, _ = splitext(basename(srcjson))
    workdir = mkdtemp(prefix='cache-')
    logger = getLogger('openaddr')
    tmpjson = _tmp_json(workdir, srcjson, extras)

    #
    # Run openaddresses-cache from a fresh working directory.
    #
    errpath = join(destdir, source+'-cache.stderr')
    outpath = join(destdir, source+'-cache.stdout')
    st_path = join(destdir, source+'-cache.status')

    with open(errpath, 'w') as stderr, open(outpath, 'w') as stdout:
        index_js = join(paths.cache, 'index.js')
        cmd_args = dict(cwd=workdir, env=s3.toenv(), stderr=stderr, stdout=stdout)

        logger.debug('openaddresses-cache {0} {1}'.format(tmpjson, workdir))

        cmd = Popen(('node', index_js, tmpjson, workdir, s3.bucketname), **cmd_args)
        _wait_for_it(cmd, 7200)

        with open(st_path, 'w') as file:
            file.write(str(cmd.returncode))

    logger.debug('{0} --> {1}'.format(source, workdir))

    with open(tmpjson) as file:
        data = json.load(file)
        
    rmtree(workdir)
    
    with open(st_path) as status, open(errpath) as err, open(outpath) as out:
        args = status.read().strip(), err.read().strip(), out.read().strip()
        output = '{}\n\nSTDERR:\n\n{}\n\nSTDOUT:\n\n{}\n'.format(*args)

    return CacheResult(data.get('cache', None),
                       data.get('fingerprint', None),
                       data.get('version', None),
                       datetime.now() - start,
                       output)

def conform(srcjson, destdir, extras, s3):
    ''' Python wrapper for openaddresses-conform.

        Return a dictionary of conformed details, a CSV URL and local path:
        
          {
            "processed": URL of conformed CSV,
            "path": Local filesystem path to conformed CSV
          }
    '''
    start = datetime.now()
    source, _ = splitext(basename(srcjson))
    workdir = mkdtemp(prefix='conform-')
    logger = getLogger('openaddr')
    tmpjson = _tmp_json(workdir, srcjson, extras)

    #
    # Run openaddresses-conform from a fresh working directory.
    #
    # It tends to error silently and truncate data if it finds any existing
    # data. Also, it wants to be able to write a file called ./tmp.csv.
    #
    errpath = join(destdir, source+'-conform.stderr')
    outpath = join(destdir, source+'-conform.stdout')
    st_path = join(destdir, source+'-conform.status')

    with open(errpath, 'w') as stderr, open(outpath, 'w') as stdout:
        index_js = join(paths.conform, 'index.js')
        cmd_args = dict(cwd=workdir, env=s3.toenv(), stderr=stderr, stdout=stdout)

        logger.debug('openaddresses-conform {0} {1}'.format(tmpjson, workdir))

        cmd = Popen(('node', index_js, tmpjson, workdir, s3.bucketname), **cmd_args)
        _wait_for_it(cmd, 7200)

        with open(st_path, 'w') as file:
            file.write(str(cmd.returncode))

    logger.debug('{0} --> {1}'.format(source, workdir))

    #
    # Move resulting files to destination directory.
    #
    zip_path = join(destdir, source+'.zip')
    csv_path = join(destdir, source+'.csv')
    
    if exists(join(workdir, source+'.zip')):
        move(join(workdir, source+'.zip'), zip_path)
        logger.debug(zip_path)

    if exists(join(workdir, source, 'out.csv')):
        move(join(workdir, source, 'out.csv'), csv_path)
        logger.debug(csv_path)

    with open(tmpjson) as file:
        data = json.load(file)
        
    rmtree(workdir)
    
    with open(st_path) as status, open(errpath) as err, open(outpath) as out:
        args = status.read().strip(), err.read().strip(), out.read().strip()
        output = '{}\n\nSTDERR:\n\n{}\n\nSTDOUT:\n\n{}\n'.format(*args)

    return ConformResult(data.get('processed', None),
                         (realpath(csv_path) if exists(csv_path) else None),
                         datetime.now() - start,
                         output)

def excerpt(srcjson, destdir, extras, s3):
    ''' 
    '''
    start = datetime.now()
    source, _ = splitext(basename(srcjson))
    workdir = mkdtemp(prefix='excerpt-')
    logger = getLogger('openaddr')
    tmpjson = _tmp_json(workdir, srcjson, extras)

    #
    sample_data = None
    got = get(extras['cache'], stream=True)
    _, ext = splitext(got.url)
    
    if not ext:
        ext = guess_extension(got.headers['content-type'])
    
    cachefile = join(workdir, 'cache'+ext)
    
    if ext == '.zip':
        logger.debug('Downloading all of {cache}'.format(**extras))

        with open(cachefile, 'w') as file:
            for chunk in got.iter_content(1024**2):
                file.write(chunk)
    
        zf = ZipFile(cachefile, 'r')
        
        for name in zf.namelist():
            _, ext = splitext(name)
            
            if ext in ('.shp', '.shx', '.dbf'):
                with open(join(workdir, 'cache'+ext), 'w') as file:
                    file.write(zf.read(name))
        
        if exists(join(workdir, 'cache.shp')):
            ds = ogr.Open(join(workdir, 'cache.shp'))
        else:
            ds = None
    
    elif ext == '.json':
        logger.debug('Downloading part of {cache}'.format(**extras))

        _, host, path, query, _, _ = urlparse(got.url)
        
        conn = HTTPConnection(host, 80)
        conn.request('GET', path + ('?' if query else '') + query)
        resp = conn.getresponse()
        
        with open(cachefile, 'w') as file:
            file.write(sample_geojson(resp, 10))
    
        ds = ogr.Open(cachefile)
    
    else:
        ds = None
    
    if ds:
        layer = ds.GetLayer(0)
        defn = layer.GetLayerDefn()
        field_count = defn.GetFieldCount()
        sample_data = [[defn.GetFieldDefn(i).name for i in range(field_count)]]
        geometry_type = geometry_types.get(defn.GetGeomType(), None)
        
        for feature in layer:
            sample_data += [[feature.GetField(i) for i in range(field_count)]]
            
            if len(sample_data) == 6:
                break
        
        #
        # Close it like in
        # http://trac.osgeo.org/gdal/wiki/PythonGotchas#Savingandclosingdatasetsdatasources
        #
        defn = None
        layer = None
        ds = None
    
    rmtree(workdir)
    
    dir = datetime.now().strftime('%Y%m%d')
    key = s3.new_key(join(dir, 'samples', source+'.json'))
    args = dict(policy='public-read', headers={'Content-Type': 'text/json'})
    key.set_contents_from_string(json.dumps(sample_data, indent=2), **args)
    
    return ExcerptResult('http://s3.amazonaws.com/{}/{}'.format(s3.bucketname, key.name),
                         geometry_type)

def _wait_for_it(command, seconds):
    ''' Run command for a limited number of seconds, then kill it.
    '''
    due = time() + seconds
    
    while True:
        if command.poll() is not None:
            # Command complete
            break
        
        elif time() > due:
            # Went overtime
            command.kill()

        else:
            # Keep waiting
            sleep(.5)

def _tmp_json(workdir, srcjson, extras):
    ''' Work on a copy of source JSON in a safe directory, with extras grafted in.
    
        Return path to the new JSON file.
    '''
    mkdir(join(workdir, 'source'))
    tmpjson = join(workdir, 'source', basename(srcjson))

    with open(srcjson, 'r') as src_file, open(tmpjson, 'w') as tmp_file:
        data = json.load(src_file)
        data.update(extras)
        json.dump(data, tmp_file)
    
    return tmpjson
