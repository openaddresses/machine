from __future__ import absolute_import, division, print_function
import logging; _L = logging.getLogger('openaddr')

from .compat import standard_library

from tempfile import mkdtemp, mkstemp
from os.path import realpath, join, basename, splitext, exists, dirname, abspath, relpath
from shutil import copy, move, rmtree
from os import mkdir, environ, close, utime
from urllib.parse import urlparse
from datetime import datetime, date
from calendar import timegm
import json, io, zipfile

from osgeo import ogr
from requests import get
from boto import connect_s3
from dateutil.parser import parse
from .sample import sample_geojson

from .cache import (
    CacheResult,
    compare_cache_details,
    DownloadTask,
    URLDownloadTask,
)

from .conform import (
    ConformResult,
    DecompressionTask,
    ExcerptDataTask,
    ConvertToCsvTask,
    elaborate_filenames,
)

with open(join(dirname(__file__), 'VERSION')) as file:
    __version__ = file.read().strip()

# Deprecated location for sources from old batch mode.
SOURCES_DIR = '/var/opt/openaddresses'

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
    resultdir = join(destdir, 'cached')
    data['cache'], data['fingerprint'] \
        = compare_cache_details(filepath_to_upload, resultdir, data)

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

    task1 = URLDownloadTask(source)
    downloaded_path = task1.download(source_urls, workdir)
    _L.info("Downloaded to %s", downloaded_path)

    task2 = DecompressionTask.from_type_string(data.get('compression'))
    names = elaborate_filenames(data.get('conform', {}).get('file', None))
    decompressed_paths = task2.decompress(downloaded_path, workdir, names)
    _L.info("Decompressed to %d files", len(decompressed_paths))

    task3 = ExcerptDataTask()
    try:
        conform = data.get('conform', {})
        data_sample, geometry_type = task3.excerpt(decompressed_paths, workdir, conform)
        _L.info("Sampled %d records", len(data_sample))
    except Exception as e:
        _L.warning("Error doing excerpt; skipping", exc_info=True)
        data_sample = None
        geometry_type = None

    task4 = ConvertToCsvTask()
    try:
        csv_path, addr_count = task4.convert(data, decompressed_paths, workdir)
        _L.info("Converted to %s with %d addresses", csv_path, addr_count)
    except Exception as e:
        _L.warning("Error doing conform; skipping", exc_info=True)
        csv_path, addr_count = None, 0

    out_path = None
    if csv_path is not None and exists(csv_path):
        move(csv_path, join(destdir, 'out.csv'))
        out_path = realpath(join(destdir, 'out.csv'))

    rmtree(workdir)

    return ConformResult(data.get('processed', None),
                         data_sample,
                         data.get('website'),
                         data.get('license'),
                         geometry_type,
                         addr_count,
                         out_path,
                         datetime.now() - start)

def package_output(source, processed_path, website, license):
    ''' Write a zip archive to temp dir with processed data and optional .vrt.
    '''
    _, ext = splitext(processed_path)
    handle, zip_path = mkstemp(suffix='.zip')
    close(handle)
    
    zip_file = zipfile.ZipFile(zip_path, mode='w', compression=zipfile.ZIP_DEFLATED)
    
    template = join(dirname(__file__), 'templates', 'README.txt')
    with io.open(template, encoding='utf8') as file:
        content = file.read().format(website=website, license=license, date=date.today())
        zip_file.writestr('README.txt', content.encode('utf8'))

    if ext == '.csv':
        # Add virtual format to make CSV readable by QGIS, OGR, etc.
        # More information: http://www.gdal.org/drv_vrt.html
        template = join(dirname(__file__), 'templates', 'conform-result.vrt')
        with io.open(template, encoding='utf8') as file:
            content = file.read().format(source=basename(source))
            zip_file.writestr(source + '.vrt', content.encode('utf8'))
    
    zip_file.write(processed_path, source + ext)
    zip_file.close()
    
    return zip_path

def iterate_local_processed_files(runs):
    ''' Yield a stream of local processed result files for a list of runs.
    '''
    key = lambda run: run.datetime_tz or date(1970, 1, 1)
    
    for run in sorted(runs, key=key, reverse=True):
        source_base, _ = splitext(relpath(run.source_path, 'sources'))
        processed_url = run.state and run.state.get('processed')
        run_state = run.state
    
        if not processed_url:
            continue
        
        try:
            filename = download_processed_file(processed_url)
        
        except:
            _L.error('Failed to download {}'.format(processed_url))
            continue
        
        else:
            yield (source_base, filename, run_state)

            if filename and exists(filename):
                remove(filename)
    
def download_processed_file(url):
    ''' Download a URL to a local temporary file, return its path.
    
        Local file will have an appropriate timestamp and extension.
    '''
    _, ext = splitext(urlparse(url).path)
    handle, filename = mkstemp(prefix='processed-', suffix=ext)
    close(handle)
    
    response = get(url, stream=True, timeout=5)
    
    with open(filename, 'wb') as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)
    
    last_modified = response.headers.get('Last-Modified')
    timestamp = timegm(parse(last_modified).utctimetuple())
    utime(filename, (timestamp, timestamp))
    
    return filename
