from __future__ import absolute_import, division, print_function
from future import standard_library; standard_library.install_aliases()
import logging
_L = logging.getLogger('openaddr')

from tempfile import mkdtemp
from os.path import realpath, join, basename, splitext, exists, dirname, abspath
from shutil import copy, move, rmtree
from datetime import datetime
from os import mkdir, environ
from urllib.parse import urlparse
import json

from osgeo import ogr
from requests import get
from boto import connect_s3
from .sample import sample_geojson
from . import paths

from .cache import (
    CacheResult,
    DownloadTask,
    URLDownloadTask,
)

from .conform import (
    ConformResult,
    DecompressionTask,
    ExcerptDataTask,
    ConvertToCsvTask,
)

with open(join(dirname(__file__), 'VERSION')) as file:
    __version__ = file.read().strip()

class S3:
    _bucket = None

    def __init__(self, key, secret, bucketname):
        self._key, self._secret = key, secret
        self.bucketname = bucketname
    
    def _make_bucket(self):
        if not self._bucket:
            connection = connect_s3(self._key, self._secret)
            self._bucket = connection.get_bucket(self.bucketname)
    
    def get_key(self, name):
        self._make_bucket()
        return self._bucket.get_key(name)
    
    def new_key(self, name):
        self._make_bucket()
        return self._bucket.new_key(name)

def cache(srcjson, destdir, extras):
    ''' Python wrapper for openaddress-cache.
    
        Return a CacheResult object:

          cache: URL of cached data, possibly with file:// schema
          fingerprint: md5 hash of data,
          version: data version as date?
          elapsed: elapsed time as timedelta object
          output: subprocess output as string
        
        Creates and destroys a subdirectory in destdir.
    '''
    start = datetime.now()
    source, _ = splitext(basename(srcjson))
    workdir = mkdtemp(prefix='cache-', dir=destdir)
    
    with open(srcjson, 'r') as src_file:
        data = json.load(src_file)
        data.update(extras)
    
    #
    #
    #
    source_urls = data.get('data')
    if not isinstance(source_urls, list):
        source_urls = [source_urls]

    task = DownloadTask.from_type_string(data.get('type'), source)
    downloaded_files = task.download(source_urls, workdir)

    # FIXME: I wrote the download stuff to assume multiple files because
    # sometimes a Shapefile fileset is splayed across multiple files instead
    # of zipped up nicely. When the downloader downloads multiple files,
    # we should zip them together before uploading to S3 instead of picking
    # the first one only.
    filepath_to_upload = abspath(downloaded_files[0])

    #
    # Find the cached data and hold on to it.
    #
    cache_name = basename(filepath_to_upload)
    if exists(filepath_to_upload):
        resultdir = join(destdir, 'cached')
        if not exists(resultdir):
            mkdir(resultdir)
        move(filepath_to_upload, join(resultdir, cache_name))
        if 'cache' not in data:
            data['cache'] = 'file://' + join(abspath(resultdir), cache_name)

    rmtree(workdir)

    return CacheResult(data.get('cache', None),
                       data.get('fingerprint', None),
                       data.get('version', None),
                       datetime.now() - start)

def conform(srcjson, destdir, extras):
    ''' Python wrapper for openaddresses-conform.
    
        Return a ConformResult object:

          processed: URL of processed data CSV
          path: local path to CSV of processed data
          geometry_type: typically Point or Polygon
          elapsed: elapsed time as timedelta object
          output: subprocess output as string
        
        Creates and destroys a subdirectory in destdir.
    '''
    start = datetime.now()
    source, _ = splitext(basename(srcjson))
    workdir = mkdtemp(prefix='conform-', dir=destdir)
    
    with open(srcjson, 'r') as src_file:
        data = json.load(src_file)
        data.update(extras)
    
    #
    # The cached data will be a local path.
    #
    scheme, _, cache_path, _, _, _ = urlparse(extras.get('cache', ''))
    if scheme == 'file':
        copy(cache_path, workdir)

    source_urls = data.get('cache')
    if not isinstance(source_urls, list):
        source_urls = [source_urls]

    task = URLDownloadTask(source)
    downloaded_path = task.download(source_urls, workdir)
    _L.info("Downloaded to %s", downloaded_path)

    task = DecompressionTask.from_type_string(data.get('compression'))
    decompressed_paths = task.decompress(downloaded_path, workdir)
    _L.info("Decompressed to %d files", len(decompressed_paths))

    task3 = ExcerptDataTask()
    try:
        sample_path, geometry_type = task3.excerpt(decompressed_paths, workdir)
        _L.info("Sampled %d records", len(sample_path))
    except TypeError as e:
        _L.warning("Error doing excerpt; skipping")
        sample_path = None
        geometry_type = None

    task = ConvertToCsvTask()
    csv_path = task.convert(data, decompressed_paths, workdir)
    _L.info("Converted to %s", csv_path)

    out_path = None
    if csv_path is not None and exists(csv_path):
        move(csv_path, join(destdir, 'out.csv'))
        out_path = realpath(join(destdir, 'out.csv'))

    rmtree(workdir)

    return ConformResult(data.get('processed', None),
                         sample_path,
                         geometry_type,
                         out_path,
                         datetime.now() - start)
