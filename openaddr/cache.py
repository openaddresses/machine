from __future__ import absolute_import, division, print_function
from future import standard_library; standard_library.install_aliases()
import logging; _L = logging.getLogger('openaddr.cache')

import json
import os
import errno
import socket
import mimetypes
import shutil

from re import search
from os.path import join, dirname
from urllib.parse import urlencode, urlparse, urljoin
from subprocess import check_output
from tempfile import mkstemp
from hashlib import sha1

import requests
import requests_ftp
requests_ftp.monkeypatch_session()

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

    # needed by openaddr.process.write_state(), for now.
    output = ''

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
    likely_ext = None

    if not query:
        _, likely_ext = os.path.splitext(path)
    
    # Get a dictionary of headers and a few bytes of content from the URL.
    if scheme in ('http', 'https'):
        response = requests.get(url, stream=True)
        content_chunk = response.iter_content(99).next()
        headers = response.headers
        response.close()
    elif scheme in ('file', ''):
        headers = dict()
        with open(path) as file:
            content_chunk = file.read(99)
    else:
        raise ValueError('Unknown scheme "{}": {}'.format(scheme, url))
    
    if likely_ext not in (None, '', '.cgi', '.php', '.aspx', '.asp', '.do'):
        #
        # Rule out missing or meaningless filename extensions.
        #
        _L.debug('URL says "{}" for {}'.format(likely_ext, url))
        path_ext = likely_ext
    
    elif 'content-disposition' in headers or 'content-type' not in headers:
        #
        # Socrata recently started using Content-Disposition instead
        # of normal response headers so it's no longer easy to identify
        # file type. Shell out to `file` to peek at the content when we're
        # unwilling to trust Content-Type header.
        #
        mime_type = get_content_mimetype(content_chunk)
        _L.debug('file says "{}" for {}'.format(mime_type, url))
        path_ext = mimetypes.guess_extension(mime_type)
    
    else:
        content_type = headers['content-type'].split(';')[0]
        _L.debug('Content-Type says "{}" for {}'.format(content_type, url))
        path_ext = mimetypes.guess_extension(content_type)
    
    return path_ext

def get_content_mimetype(chunk):
    ''' Get a mime-type for a short length of file content.
    '''
    handle, file = mkstemp()
    os.write(handle, chunk)
    os.close(handle)
    
    mime_type = check_output(('file', '--mime-type', '-b', file)).strip()
    os.remove(file)
    
    return mime_type

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
                resp = requests.get(source_url, headers=headers, stream=True)
            except Exception as e:
                raise DownloadError("Could not connect to URL", e)

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

    def convert_esrijson_to_geojson(self, geom_type, esri_feature):
        if geom_type == 'esriGeometryPoint':
            geometry = {
                "type": "Point",
                "coordinates": [
                    esri_feature['geometry']['x'],
                    esri_feature['geometry']['y']
                ]
            }
        elif geom_type == 'esriGeometryMultipoint':
            geometry = {
                "type": "MultiPoint",
                "coordinates": [
                    [geom[0], geom[1]] for geom in esri_feature['geometry']['points']
                ]
            }
        elif geom_type == 'esriGeometryPolygon':
            geometry = {
                "type": "Polygon",
                "coordinates": [
                    [
                        [geom[0], geom[1]] for geom in ring
                    ] for ring in esri_feature['geometry']['rings']
                ]
            }
        elif geom_type == 'esriGeometryPolyline':
            geometry = {
                "type": "MultiLineString",
                "coordinates": [
                    [
                        [geom[0], geom[1]] for geom in path
                    ] for path in esri_feature['geometry']['paths']
                ]
            }
        else:
            raise KeyError("Don't know how to convert esri geometry type {}".format(geom_type))

        return {
            "type": "Feature",
            "properties": esri_feature.get('attributes'),
            "geometry": geometry
        }

    def get_file_path(self, url, dir_path):
        ''' Return a local file path in a directory for a URL.
        '''
        _, host, path, _, _, _ = urlparse(url)
        hash, path_ext = sha1((host + path).encode('utf-8')), '.json'

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

            oid_field = 'objectid'
            response = requests.get(source_url, params={'f': 'json'})
            for field in response.json().get('fields', []):
                if field.get('type') == 'esriFieldTypeOID':
                    oid_field = field.get('name')
                    break

            with open(file_path, 'w') as f:
                f.write('{\n"type": "FeatureCollection",\n"features": [\n')
                start = 0
                width = 500
                while True:
                    query_url = source_url + '/query'
                    query_args = {
                        'where': '{oid_field} >= {start} and {oid_field} < {end}'.format(
                            oid_field=oid_field,
                            start=start,
                            end=(start + width)
                        ),
                        'geometryPrecision': 7,
                        'returnGeometry': True,
                        'outSR': 4326,
                        'outFields': '*',
                        'f': 'JSON',
                    }

                    headers = {'User-Agent': self.USER_AGENT}

                    try:
                        response = requests.get(query_url, headers=headers, params=query_args)
                        _L.debug("Requesting %s", response.url)
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
                        raise DownloadError("Problem querying ESRI dataset: %s", error['message'])

                    geometry_type = data.get('geometryType')
                    features = data.get('features')

                    f.write(',\n'.join([
                        json.dumps(self.convert_esrijson_to_geojson(geometry_type, feature)) for feature in features
                    ]))

                    size += len(features)
                    if len(features) == 0:
                        break
                    else:
                        f.write(',\n')
                        start += width

                f.write('\n]\n}\n')
            _L.info("Downloaded %s ESRI features for file %s", size, file_path)
            output_files.append(file_path)
        return output_files

import unittest, httmock

class TestCacheExtensionGuessing (unittest.TestCase):

    def response_content(self, url, request):
        ''' Fake HTTP responses for use with HTTMock in tests.
        '''
        scheme, host, path, _, query, _ = urlparse(url.geturl())
        tests_dirname = join(os.getcwd(), 'tests')
        
        if host == 'fake-cwd.local':
            with open(tests_dirname + path) as file:
                type, _ = mimetypes.guess_type(file.name)
                return httmock.response(200, file.read(), headers={'Content-Type': type})
        
        elif (host, path) == ('www.ci.berkeley.ca.us', '/uploadedFiles/IT/GIS/Parcels.zip'):
            with open(join(tests_dirname, 'data', 'us-ca-berkeley-excerpt.zip')) as file:
                return httmock.response(200, file.read(), headers={'Content-Type': 'application/octet-stream'})

        elif (host, path) == ('data.sfgov.org', '/download/kvej-w5kb/ZIPPED%20SHAPEFILE'):
            return httmock.response(302, '', headers={'Location': 'http://apps.sfgov.org/datafiles/view.php?file=sfgis/eas_addresses_with_units.zip'})

        elif (host, path, query) == ('apps.sfgov.org', '/datafiles/view.php', 'file=sfgis/eas_addresses_with_units.zip'):
            with open(join(tests_dirname, 'data', 'us-ca-san_francisco-excerpt.zip')) as file:
                return httmock.response(200, file.read(), headers={'Content-Type': 'application/download', 'Content-Disposition': 'attachment; filename=eas_addresses_with_units.zip;'})

        raise NotImplementedError(url.geturl())
    
    def test_urls(self):
        with httmock.HTTMock(self.response_content):
            assert guess_url_file_extension('http://fake-cwd.local/conforms/lake-man-3740.csv') == '.csv'
            assert guess_url_file_extension('http://fake-cwd.local/data/us-ca-carson-0.json') == '.json'
            assert guess_url_file_extension('http://fake-cwd.local/data/us-ca-oakland-excerpt.zip') == '.zip'
            assert guess_url_file_extension('http://www.ci.berkeley.ca.us/uploadedFiles/IT/GIS/Parcels.zip') == '.zip'
            assert guess_url_file_extension('https://data.sfgov.org/download/kvej-w5kb/ZIPPED%20SHAPEFILE') == '.zip'
