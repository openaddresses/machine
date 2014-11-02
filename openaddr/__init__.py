from subprocess import Popen
from multiprocessing import Process
from tempfile import mkdtemp
from os.path import realpath, join, basename, splitext, exists
from shutil import copy, move, rmtree
from mimetypes import guess_extension
from StringIO import StringIO
from logging import getLogger
from datetime import datetime
from os import mkdir, environ
from time import sleep, time
from zipfile import ZipFile
import json

from osgeo import ogr
from requests import get
from boto import connect_s3
from . import paths
from openaddr.cache import (
    CacheResult,
    DownloadTask,
    DecompressionTask,
    ConvertToCsvTask,
    upload_to_s3
)

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

    def __init__(self, sample_data):
        self.sample_data = sample_data
    
    @staticmethod
    def empty():
        return ExcerptResult(None)

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

    def thread_work(json_filepath, source_key):
        with open(json_filepath, 'r') as j:
            data = json.load(j)

        source_urls = data.get('data')
        if not isinstance(source_urls, list):
            source_urls = [source_urls]

        task = DownloadTask.from_type_string(data.get('type'))
        downloaded_files = task.download(source_urls, workdir)

        # FIXME: I wrote the download stuff to assume multiple files because
        # sometimes a Shapefile fileset is splayed across multiple files instead
        # of zipped up nicely. When the downloader downloads multiple files,
        # we should zip them together before uploading to S3 instead of picking
        # the first one only.
        filepath_to_upload = downloaded_files[0]

        version = datetime.utcnow().strftime('%Y%m%d')
        key = '/{}/{}'.format(version, basename(filepath_to_upload))

        k = upload_to_s3(bucketname, key, filepath_to_upload)

        data['cache'] = k.generate_url(expires_in=0, query_auth=False)
        data['fingerprint'] = k.md5
        data['version'] = version

        with open(json)

    p = Process(target=thread_work, args=(tmpjson, source), name='oa-cache-'+source)
    p.start()
    # FIXME: We could add an integer argument to join() for the number of seconds
    # to wait for this process to finish
    p.join()

    rmtree(workdir)

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
    got = get(extras['cache'])
    _, ext = splitext(got.url)
    
    if not ext:
        ext = guess_extension(got.headers['content-type'])
    
    if ext == '.zip':
        zbuff = StringIO(got.content)
        zf = ZipFile(zbuff, 'r')
        
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
        with open(join(workdir, 'cache.json'), 'w') as file:
            file.write(got.content)
        
        ds = ogr.Open(join(workdir, 'cache.json'))
    
    else:
        ds = None
    
    if ds:
        layer = ds.GetLayer(0)
        defn = layer.GetLayerDefn()
        field_count = defn.GetFieldCount()
        sample_data = [[defn.GetFieldDefn(i).name for i in range(field_count)]]
        
        for feature in layer:
            sample_data += [[feature.GetField(i) for i in range(field_count)]]
            
            if len(sample_data) == 6:
                break
    
    rmtree(workdir)
    
    dir = datetime.now().strftime('%Y%m%d')
    key = s3.new_key(join(dir, 'samples', source+'.json'))
    args = dict(policy='public-read', headers={'Content-Type': 'text/json'})
    key.set_contents_from_string(json.dumps(sample_data, indent=2), **args)
    
    return ExcerptResult('http://s3.amazonaws.com/{}/{}'.format(s3.bucketname, key.name))

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
