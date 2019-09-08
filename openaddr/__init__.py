from __future__ import absolute_import, division, print_function
import logging; _L = logging.getLogger('openaddr')

from tempfile import mkdtemp, mkstemp
from os.path import realpath, join, basename, splitext, exists, dirname, abspath, relpath
from shutil import copy, move, rmtree
from os import close, utime, remove
from urllib.parse import urlparse
from datetime import datetime, date
from calendar import timegm
import json
import requests

from osgeo import ogr
from boto.s3.connection import S3Connection
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
    conform_license,
    conform_attribution,
    conform_sharealike,
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
            # see https://github.com/boto/boto/issues/2836#issuecomment-67896932
            kwargs = dict(calling_format='boto.s3.connection.OrdinaryCallingFormat')
            connection = S3Connection(self._key, self._secret, **kwargs)
            self._bucket = connection.get_bucket(self.bucketname)

    @property
    def bucket(self):
        self._make_bucket()
        return self._bucket

    def get_key(self, name):
        return self.bucket.get_key(name)

    def new_key(self, name):
        return self.bucket.new_key(name)

class LocalProcessedResult:
    def __init__(self, source_base, filename, run_state, code_version):
        for attr in ('attribution_name', 'attribution_flag', 'website', 'license'):
            assert hasattr(run_state, attr), 'Run state should have {} property'.format(attr)

        self.source_base = source_base
        self.filename = filename
        self.run_state = run_state
        self.code_version = code_version

def cache(data_source_name, data_source, destdir, extras):
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
    workdir = mkdtemp(prefix='cache-', dir=destdir)

    data_source.update(extras)

    source_urls = data_source.get('data')
    if not isinstance(source_urls, list):
        source_urls = [source_urls]

    protocol_string = data_source.get('protocol')

    task = DownloadTask.from_protocol_string(protocol_string, data_source_name)
    downloaded_files = task.download(source_urls, workdir, data_source.get('conform'))

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
    data_source['cache'], data_source['fingerprint'] \
        = compare_cache_details(filepath_to_upload, resultdir, data_source)

    rmtree(workdir)

    return CacheResult(data_source.get('cache', None),
                       data_source.get('fingerprint', None),
                       data_source.get('version', None),
                       datetime.now() - start)

def conform(data_source_name, data_source, destdir, extras):
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
    workdir = mkdtemp(prefix='conform-', dir=destdir)

    data_source.update(extras)

    #
    # The cached data will be a local path.
    #
    scheme, _, cache_path, _, _, _ = urlparse(extras.get('cache', ''))
    if scheme == 'file':
        copy(cache_path, workdir)

    source_urls = data_source.get('cache')
    if not isinstance(source_urls, list):
        source_urls = [source_urls]

    task1 = URLDownloadTask(data_source_name)
    downloaded_path = task1.download(source_urls, workdir)
    _L.info("Downloaded to %s", downloaded_path)

    task2 = DecompressionTask.from_format_string(data_source.get('compression'))
    names = elaborate_filenames(data_source.get('conform', {}).get('file', None))
    decompressed_paths = task2.decompress(downloaded_path, workdir, names)
    _L.info("Decompressed to %d files", len(decompressed_paths))

    task3 = ExcerptDataTask()
    try:
        conform = data_source.get('conform', {})
        data_sample, geometry_type = task3.excerpt(decompressed_paths, workdir, conform)
        _L.info("Sampled %d records", len(data_sample))
    except Exception as e:
        _L.warning("Error doing excerpt; skipping", exc_info=True)
        data_sample = None
        geometry_type = None

    task4 = ConvertToCsvTask()
    try:
        csv_path, addr_count = task4.convert(data_source, decompressed_paths, workdir)
        if addr_count > 0:
            _L.info("Converted to %s with %d addresses", csv_path, addr_count)
        else:
            _L.warning('Found no addresses in source data')
            csv_path = None
    except Exception as e:
        _L.warning("Error doing conform; skipping", exc_info=True)
        csv_path, addr_count = None, 0

    out_path = None
    if csv_path is not None and exists(csv_path):
        move(csv_path, join(destdir, 'out.csv'))
        out_path = realpath(join(destdir, 'out.csv'))

    rmtree(workdir)

    sharealike_flag = conform_sharealike(data_source.get('license'))
    attr_flag, attr_name = conform_attribution(data_source.get('license'), data_source.get('attribution'))

    return ConformResult(data_source.get('processed', None),
                         data_sample,
                         data_source.get('website'),
                         conform_license(data_source.get('license')),
                         geometry_type,
                         addr_count,
                         out_path,
                         datetime.now() - start,
                         sharealike_flag,
                         attr_flag,
                         attr_name)

def iterate_local_processed_files(runs, sort_on='datetime_tz'):
    ''' Yield a stream of local processed result files for a list of runs.

        Used in ci.collect and dotmap processes.
    '''
    if sort_on == 'source_path':
        reverse, key = False, lambda run: run.source_path
    else:
        reverse, key = True, lambda run: run.datetime_tz or date(1970, 1, 1)

    for run in sorted(runs, key=key, reverse=reverse):
        source_base, _ = splitext(relpath(run.source_path, 'sources'))
        processed_url = run.state and run.state.processed
        run_state = run.state

        if not processed_url:
            continue

        try:
            filename = download_processed_file(processed_url)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                continue
            else:
                _L.error('HTTP {} while downloading {}: {}'.format(e.response.status_code, processed_url, e))
                continue
        except Exception as e:
            _L.error('Failed to download {}: {}'.format(processed_url, e))
            continue

        yield LocalProcessedResult(source_base, filename, run_state, run.code_version)

        if filename and exists(filename):
            remove(filename)

def download_processed_file(url):
    ''' Download a URL to a local temporary file, return its path.

        Local file will have an appropriate timestamp and extension.
    '''
    _, ext = splitext(urlparse(url).path)
    handle, filename = mkstemp(prefix='processed-', suffix=ext)
    close(handle)

    response = requests.get(url, stream=True, timeout=5)
    response.raise_for_status()

    with open(filename, 'wb') as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)

    last_modified = response.headers.get('Last-Modified')
    timestamp = timegm(parse(last_modified).utctimetuple())
    utime(filename, (timestamp, timestamp))

    return filename
