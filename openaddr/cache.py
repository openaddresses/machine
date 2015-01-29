from __future__ import absolute_import, division, print_function
import logging; _L = logging.getLogger('openaddr.cache')

from .compat import standard_library

import ogr
import sys
import os
import errno
import socket
import mimetypes
import shutil
import itertools

from os import mkdir
from hashlib import md5
from os.path import join, basename, exists, abspath, dirname
from urllib.parse import urlparse, parse_qs
from subprocess import check_output
from tempfile import mkstemp
from hashlib import sha1
from shutil import move
from time import time

import requests
import requests_ftp
requests_ftp.monkeypatch_session()

# HTTP timeout in seconds, used in various calls to requests.get() and requests.post()
_http_timeout = 180

from .compat import csvopen, csvDictWriter

def mkdirsp(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


class CacheResult:
    cache = None
    fingerprint = None
    version = None
    elapsed = None

    def __init__(self, cache, fingerprint, version, elapsed):
        self.cache = cache
        self.fingerprint = fingerprint
        self.version = version
        self.elapsed = elapsed

    @staticmethod
    def empty():
        return CacheResult(None, None, None, None)

    def todict(self):
        return dict(cache=self.cache, fingerprint=self.fingerprint, version=self.version)


def compare_cache_details(filepath, resultdir, data):
    ''' Compare cache file with known source data, return cache and fingerprint.
    
        Checks if fresh data is already cached, returns a new file path if not.
    '''
    if not exists(filepath):
        raise Exception('cached file {} is missing'.format(filepath))
        
    fingerprint = md5()

    with open(filepath, 'rb') as file:
        for line in file:
            fingerprint.update(line)
    
    # Determine if anything needs to be done at all.
    if urlparse(data.get('cache', '')).scheme == 'http' and 'fingerprint' in data:
        if fingerprint.hexdigest() == data['fingerprint']:
            return data['cache'], data['fingerprint']
    
    cache_name = basename(filepath)

    if not exists(resultdir):
        mkdir(resultdir)

    move(filepath, join(resultdir, cache_name))
    data_cache = 'file://' + join(abspath(resultdir), cache_name)
    
    return data_cache, fingerprint.hexdigest()

class DownloadError(Exception):
    pass


class DownloadTask(object):

    def __init__(self, source_prefix):
        self.source_prefix = source_prefix

    @classmethod
    def from_type_string(clz, type_string, source_prefix=None):
        if type_string.lower() == 'http':
            return URLDownloadTask(source_prefix)
        elif type_string.lower() == 'ftp':
            return URLDownloadTask(source_prefix)
        elif type_string.lower() == 'esri':
            return EsriRestDownloadTask(source_prefix)
        else:
            raise KeyError("I don't know how to extract for type {}".format(type_string))

    def download(self, source_urls, workdir):
        raise NotImplementedError()

def guess_url_file_extension(url):
    ''' Get a filename extension for a URL using various hints.
    '''
    scheme, _, path, _, query, _ = urlparse(url)
    mimetypes.add_type('application/x-zip-compressed', '.zip', False)
    
    _, likely_ext = os.path.splitext(path)
    bad_extensions = '', '.cgi', '.php', '.aspx', '.asp', '.do'
    
    if not query and likely_ext not in bad_extensions:
        #
        # Trust simple URLs without meaningless filename extensions.
        #
        _L.debug('URL says "{}" for {}'.format(likely_ext, url))
        path_ext = likely_ext
    
    else:
        #
        # Get a dictionary of headers and a few bytes of content from the URL.
        #
        if scheme in ('http', 'https'):
            response = requests.get(url, stream=True, timeout=_http_timeout)
            content_chunk = next(response.iter_content(99))
            headers = response.headers
            response.close()
        elif scheme in ('file', ''):
            headers = dict()
            with open(path) as file:
                content_chunk = file.read(99)
        else:
            raise ValueError('Unknown scheme "{}": {}'.format(scheme, url))
    
        if 'content-disposition' in headers or 'content-type' not in headers:
            #
            # Socrata recently started using Content-Disposition instead
            # of normal response headers so it's no longer easy to identify
            # file type. Shell out to `file` to peek at the content when we're
            # unwilling to trust Content-Type header.
            #
            mime_type = get_content_mimetype(content_chunk)
            _L.debug('file says "{}" for {}'.format(mime_type, url))
            path_ext = mimetypes.guess_extension(mime_type, False)
    
        else:
            content_type = headers['content-type'].split(';')[0]
            _L.debug('Content-Type says "{}" for {}'.format(content_type, url))
            path_ext = mimetypes.guess_extension(content_type, False)
    
    return path_ext

def get_content_mimetype(chunk):
    ''' Get a mime-type for a short length of file content.
    '''
    handle, file = mkstemp()
    os.write(handle, chunk)
    os.close(handle)
    
    mime_type = check_output(('file', '--mime-type', '-b', file)).strip()
    os.remove(file)
    
    return mime_type.decode('utf-8')

class URLDownloadTask(DownloadTask):
    USER_AGENT = 'openaddresses-extract/1.0 (https://github.com/openaddresses/openaddresses)'
    CHUNK = 16 * 1024

    def get_file_path(self, url, dir_path):
        ''' Return a local file path in a directory for a URL.

            May need to fill in a filename extension based on HTTP Content-Type.
        '''
        scheme, host, path, _, _, _ = urlparse(url)
        path_base, _ = os.path.splitext(path)

        if self.source_prefix is None:
            # With no source prefix like "us-ca-oakland" use the name as given.
            name_base = os.path.basename(path_base)
        else:
            # With a source prefix, create a safe and unique filename with a hash.
            hash = sha1((host + path_base).encode('utf-8'))
            name_base = '{}-{}'.format(self.source_prefix, hash.hexdigest()[:8])
        
        path_ext = guess_url_file_extension(url)
        _L.debug('Guessed {}{} for {}'.format(name_base, path_ext, url))
    
        return os.path.join(dir_path, name_base + path_ext)

    def download(self, source_urls, workdir):
        output_files = []
        download_path = os.path.join(workdir, 'http')
        mkdirsp(download_path)

        for source_url in source_urls:
            file_path = self.get_file_path(source_url, download_path)

            # FIXME: For URLs with file:// scheme, simply copy the file
            # to the expected location so that os.path.exists() returns True.
            # Instead, implement a FileDownloadTask class?
            scheme, _, path, _, _, _ = urlparse(source_url)
            if scheme == 'file':
                shutil.copy(path, file_path)

            if os.path.exists(file_path):
                output_files.append(file_path)
                _L.debug("File exists %s", file_path)
                continue

            _L.info("Requesting %s", source_url)
            headers = {'User-Agent': self.USER_AGENT}

            try:
                resp = requests.get(source_url, headers=headers, stream=True, timeout=_http_timeout)
            except Exception as e:
                raise DownloadError("Could not connect to URL", e)

            if resp.status_code in range(400, 499):
                raise DownloadError('{} response from {}'.format(resp.status_code, source_url))
            
            size = 0
            with open(file_path, 'wb') as fp:
                for chunk in resp.iter_content(self.CHUNK):
                    size += len(chunk)
                    fp.write(chunk)

            output_files.append(file_path)

            _L.info("Downloaded %s bytes for file %s", size, file_path)

        return output_files


class EsriRestDownloadTask(DownloadTask):
    USER_AGENT = 'openaddresses-extract/1.0 (https://github.com/openaddresses/openaddresses)'

    def build_ogr_geometry(self, geom_type, esri_feature):
        if geom_type == 'esriGeometryPoint':
            geom = ogr.Geometry(ogr.wkbPoint)
            geom.AddPoint(esri_feature['geometry']['x'], esri_feature['geometry']['y'])
        elif geom_type == 'esriGeometryMultipoint':
            geom = ogr.Geometry(ogr.wkbMultiPoint)
            for point in esri_feature['geometry']['points']:
                pt = ogr.Geometry(ogr.wkbPoint)
                pt.AddPoint(point[0], point[1])
                geom.AddGeometry(pt)
        elif geom_type == 'esriGeometryPolygon':
            geom = ogr.Geometry(ogr.wkbPolygon)
            for esri_ring in esri_feature['geometry']['rings']:
                ring = ogr.Geometry(ogr.wkbLinearRing)
                for esri_pt in esri_ring:
                    ring.AddPoint(esri_pt[0], esri_pt[1])
                geom.AddGeometry(ring)
        elif geom_type == 'esriGeometryPolyline':
            geom = ogr.Geometry(ogr.wkbMultiLineString)
            for esri_ring in esri_feature['geometry']['rings']:
                line = ogr.Geometry(ogr.wkbLineString)
                for esri_pt in esri_ring:
                    line.AddPoint(esri_pt[0], esri_pt[1])
                geom.AddGeometry(line)

        if geom:
            return geom
        else:
            raise KeyError("Don't know how to convert esri geometry type {}".format(geom_type))

    def get_file_path(self, url, dir_path):
        ''' Return a local file path in a directory for a URL.
        '''
        _, host, path, _, _, _ = urlparse(url)
        hash, path_ext = sha1((host + path).encode('utf-8')), '.csv'

        # With no source prefix like "us-ca-oakland" use the host as a hint.
        name_base = '{}-{}'.format(self.source_prefix or host, hash.hexdigest()[:8])

        _L.debug('Downloading {} to {}{}'.format(path, name_base, path_ext))

        return os.path.join(dir_path, name_base + path_ext)

    def download(self, source_urls, workdir):
        output_files = []
        download_path = os.path.join(workdir, 'esri')
        mkdirsp(download_path)

        for source_url in source_urls:
            size = 0
            file_path = self.get_file_path(source_url, download_path)

            if os.path.exists(file_path):
                output_files.append(file_path)
                _L.debug("File exists %s", file_path)
                continue

            headers = {'User-Agent': self.USER_AGENT}

            # Get the fields
            query_args = {
                'f': 'json'
            }
            response = requests.get(source_url, params=query_args, headers=headers, timeout=_http_timeout)

            if response.status_code != 200:
                raise DownloadError('Could not retrieve field names from ESRI source: HTTP {} {}'.format(
                    response.status_code,
                    response.text
                ))

            metadata = response.json()

            error = metadata.get('error')
            if error:
                raise DownloadError("Problem querying ESRI field names: {}" .format(error['message']))
            if not metadata.get('fields'):
                raise DownloadError("No fields available in the source")

            field_names = [f['name'] for f in metadata['fields']]
            if 'X' not in field_names:
                field_names.append('X')
            if 'Y' not in field_names:
                field_names.append('Y')
            if 'geom' not in field_names:
                field_names.append('geom')

            # Get all the OIDs
            query_url = source_url + '/query'
            query_args = {
                'where': '1=1', # So we get everything
                'returnIdsOnly': 'true',
                'f': 'json',
            }
            response = requests.get(query_url, params=query_args, headers=headers, timeout=_http_timeout)

            if response.status_code != 200:
                raise DownloadError('Could not retrieve object IDs from ESRI source: HTTP {} {}'.format(
                    response.status_code,
                    response.text
                ))

            oids = response.json().get('objectIds', [])

            with csvopen(file_path, 'w', encoding='utf-8') as f:
                writer = csvDictWriter(f, fieldnames=field_names, encoding='utf-8')
                writer.writeheader()

                oid_iter = iter(oids)
                due = time() + 7200
                while True:
                    oid_chunk = tuple(itertools.islice(oid_iter, 100))

                    if not oid_chunk:
                        break
                    
                    if time() > due:
                        raise RuntimeError('Ran out of time caching Esri features')

                    query_args = {
                        'objectIds': ','.join(map(str, oid_chunk)),
                        'geometryPrecision': 7,
                        'returnGeometry': 'true',
                        'outSR': 4326,
                        'outFields': '*',
                        'f': 'json',
                    }

                    try:
                        response = requests.post(query_url, headers=headers, data=query_args, timeout=_http_timeout)
                        _L.debug("Requesting %s", response.url)

                        if response.status_code != 200:
                            raise DownloadError('Could not retrieve this chunk of objects from ESRI source: HTTP {} {}'.format(
                                response.status_code,
                                response.text
                            ))

                        data = response.json()
                    except socket.timeout as e:
                        raise DownloadError("Timeout when connecting to URL", e)
                    except ValueError as e:
                        raise DownloadError("Could not parse JSON", e)
                    except Exception as e:
                        raise DownloadError("Could not connect to URL", e)
                    finally:
                        # Wipe out whatever we had written out so far
                        f.truncate()

                    error = data.get('error')
                    if error:
                        raise DownloadError("Problem querying ESRI dataset: {}" .format(error['message']))

                    geometry_type = data.get('geometryType')
                    features = data.get('features')

                    for feature in features:
                        try:
                            ogr_geom = self.build_ogr_geometry(geometry_type, feature)
                            row = feature.get('attributes', {})
                            row['geom'] = ogr_geom.ExportToWkt()
                            try:
                                centroid = ogr_geom.Centroid()
                            except RuntimeError as e:
                                if 'Invalid number of points in LinearRing found' not in str(e):
                                    raise
                                xmin, xmax, ymin, ymax = ogr_geom.GetEnvelope()
                                row['X'] = round(xmin/2 + xmax/2, 7)
                                row['Y'] = round(ymin/2 + ymax/2, 7)
                            else:
                                row['X'] = round(centroid.GetX(), 7)
                                row['Y'] = round(centroid.GetY(), 7)

                            writer.writerow(row)
                            size += 1
                        except TypeError:
                            _L.debug("Skipping a geometry", exc_info=True)

            _L.info("Downloaded %s ESRI features for file %s", size, file_path)
            output_files.append(file_path)
        return output_files

import unittest, httmock, tempfile

class TestCacheExtensionGuessing (unittest.TestCase):

    def response_content(self, url, request):
        ''' Fake HTTP responses for use with HTTMock in tests.
        '''
        scheme, host, path, _, query, _ = urlparse(url.geturl())
        tests_dirname = join(dirname(__file__), '..', 'tests')
        
        if host == 'fake-cwd.local':
            with open(tests_dirname + path, 'rb') as file:
                type, _ = mimetypes.guess_type(file.name)
                return httmock.response(200, file.read(), headers={'Content-Type': type})
        
        elif (host, path) == ('www.ci.berkeley.ca.us', '/uploadedFiles/IT/GIS/Parcels.zip'):
            with open(join(tests_dirname, 'data', 'us-ca-berkeley-excerpt.zip'), 'rb') as file:
                return httmock.response(200, file.read(), headers={'Content-Type': 'application/octet-stream'})

        elif (host, path) == ('data.sfgov.org', '/download/kvej-w5kb/ZIPPED%20SHAPEFILE'):
            return httmock.response(302, '', headers={'Location': 'http://apps.sfgov.org/datafiles/view.php?file=sfgis/eas_addresses_with_units.zip'})

        elif (host, path, query) == ('apps.sfgov.org', '/datafiles/view.php', 'file=sfgis/eas_addresses_with_units.zip'):
            with open(join(tests_dirname, 'data', 'us-ca-san_francisco-excerpt.zip'), 'rb') as file:
                return httmock.response(200, file.read(), headers={'Content-Type': 'application/download', 'Content-Disposition': 'attachment; filename=eas_addresses_with_units.zip;'})

        elif (host, path, query) == ('dcatlas.dcgis.dc.gov', '/catalog/download.asp', 'downloadID=2182&downloadTYPE=ESRI'):
            return httmock.response(200, b'FAKE'*99, headers={'Content-Type': 'application/x-zip-compressed'})

        raise NotImplementedError(url.geturl())
    
    def test_urls(self):
        with httmock.HTTMock(self.response_content):
            assert guess_url_file_extension('http://fake-cwd.local/conforms/lake-man-3740.csv') == '.csv'
            assert guess_url_file_extension('http://fake-cwd.local/data/us-ca-carson-0.json') == '.json'
            assert guess_url_file_extension('http://fake-cwd.local/data/us-ca-oakland-excerpt.zip') == '.zip'
            assert guess_url_file_extension('http://www.ci.berkeley.ca.us/uploadedFiles/IT/GIS/Parcels.zip') == '.zip'
            assert guess_url_file_extension('https://data.sfgov.org/download/kvej-w5kb/ZIPPED%20SHAPEFILE') == '.zip'
            assert guess_url_file_extension('http://dcatlas.dcgis.dc.gov/catalog/download.asp?downloadID=2182&downloadTYPE=ESRI') == '.zip'

class TestCacheEsriDownload (unittest.TestCase):

    def setUp(self):
        ''' Prepare a clean temporary directory, and work there.
        '''
        self.workdir = tempfile.mkdtemp(prefix='testCache-')
    
    def tearDown(self):
        shutil.rmtree(self.workdir)

    def response_content(self, url, request):
        ''' Fake HTTP responses for use with HTTMock in tests.
        '''
        scheme, host, path, _, query, _ = urlparse(url.geturl())
        data_dirname = join(dirname(__file__), '..', 'tests', 'data')
        local_path = False
        
        if host == 'www.carsonproperty.info':
            qs = parse_qs(query)
            
            if path == '/ArcGIS/rest/services/basemap/MapServer/1/query':
                body_data = parse_qs(request.body) if request.body else {}

                if qs.get('returnIdsOnly') == ['true']:
                    local_path = join(data_dirname, 'us-ca-carson-ids-only.json')
                elif body_data.get('outSR') == ['4326']:
                    local_path = join(data_dirname, 'us-ca-carson-0.json')
            
            elif path == '/ArcGIS/rest/services/basemap/MapServer/1':
                if qs.get('f') == ['json']:
                    local_path = join(data_dirname, 'us-ca-carson-metadata.json')
        
        if host == 'gis.cmpdd.org':
            qs = parse_qs(query)
            
            if path == '/arcgis/rest/services/Viewers/Madison/MapServer/13/query':
                body_data = parse_qs(request.body) if request.body else {}

                if qs.get('returnIdsOnly') == ['true']:
                    local_path = join(data_dirname, 'us-ms-madison-ids-only.json')
                elif body_data.get('outSR') == ['4326']:
                    local_path = join(data_dirname, 'us-ms-madison-0.json')
            
            elif path == '/arcgis/rest/services/Viewers/Madison/MapServer/13':
                if qs.get('f') == ['json']:
                    local_path = join(data_dirname, 'us-ms-madison-metadata.json')

        if local_path:
            type, _ = mimetypes.guess_type(local_path)
            with open(local_path, 'rb') as file:
                return httmock.response(200, file.read(), headers={'Content-Type': type})
        
        raise NotImplementedError(url.geturl())
    
    def test_download_carson(self):
        with httmock.HTTMock(self.response_content):
            task = EsriRestDownloadTask('us-ca-carson')
            task.download(['http://www.carsonproperty.info/ArcGIS/rest/services/basemap/MapServer/1'], self.workdir)
    
    def test_download_madison(self):
        with httmock.HTTMock(self.response_content):
            task = EsriRestDownloadTask('us-ms-madison')
            task.download(['http://gis.cmpdd.org/arcgis/rest/services/Viewers/Madison/MapServer/13'], self.workdir)
