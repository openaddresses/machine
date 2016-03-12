from __future__ import absolute_import, division, print_function
import logging; _L = logging.getLogger('openaddr.cache')

from .compat import standard_library, PY2

import ogr
import os
import errno
import socket
import mimetypes
import shutil
import re
import simplejson as json

from os import mkdir
from hashlib import md5
from os.path import join, basename, exists, abspath, splitext
from urllib.parse import urlparse
from subprocess import check_output
from tempfile import mkstemp
from hashlib import sha1
from shutil import move

import requests
import requests_ftp
requests_ftp.monkeypatch_session()

# HTTP timeout in seconds, used in various calls to requests.get() and requests.post()
_http_timeout = 180

from .compat import csvopen, csvDictWriter
from .conform import X_FIELDNAME, Y_FIELDNAME, GEOM_FIELDNAME, attrib_types

def mkdirsp(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

def request(method, url, **kwargs):
    try:
        _L.debug("Requesting %s with args %s", url, kwargs.get('params') or kwargs.get('data'))
        return requests.request(method, url, timeout=_http_timeout, **kwargs)
    except requests.exceptions.SSLError as e:
        _L.warning("Retrying %s without SSL verification", url)
        return requests.request(method, url, timeout=_http_timeout, verify=False, **kwargs)

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
        self.headers = {
            'User-Agent': 'openaddresses-extract/1.0 (https://github.com/openaddresses/openaddresses)',
        }


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

    def download(self, source_urls, workdir, conform):
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
        _L.debug(u'URL says "{}" for {}'.format(likely_ext, url))
        path_ext = likely_ext
    
    else:
        #
        # Get a dictionary of headers and a few bytes of content from the URL.
        #
        if scheme in ('http', 'https'):
            response = request('GET', url, stream=True)
            content_chunk = next(response.iter_content(99))
            headers = response.headers
            response.close()
        elif scheme in ('file', ''):
            headers = dict()
            with open(path) as file:
                content_chunk = file.read(99)
        else:
            raise ValueError('Unknown scheme "{}": {}'.format(scheme, url))
    
        path_ext = False
        
        # Guess path extension from Content-Type header
        if 'content-type' in headers:
            content_type = headers['content-type'].split(';')[0]
            _L.debug('Content-Type says "{}" for {}'.format(content_type, url))
            path_ext = mimetypes.guess_extension(content_type, False)

            #
            # Uh-oh, see if Content-Disposition disagrees with Content-Type.
            # Socrata recently started using Content-Disposition instead
            # of normal response headers so it's no longer easy to identify
            # file type.
            #
            if 'content-disposition' in headers:
                pattern = r'attachment; filename=("?)(?P<filename>[^;]+)\1'
                match = re.match(pattern, headers['content-disposition'], re.I)
                if match:
                    _, attachment_ext = splitext(match.group('filename'))
                    if path_ext == attachment_ext:
                        _L.debug('Content-Disposition agrees: "{}"'.format(match.group('filename')))
                    else:
                        _L.debug('Content-Disposition disagrees: "{}"'.format(match.group('filename')))
                        path_ext = False
        
        if not path_ext:
            #
            # Headers didn't clearly define a known extension.
            # Instead, shell out to `file` to peek at the content.
            #
            mime_type = get_content_mimetype(content_chunk)
            _L.debug('file says "{}" for {}'.format(mime_type, url))
            path_ext = mimetypes.guess_extension(mime_type, False)
    
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
            name_base = u'{}-{}'.format(self.source_prefix, hash.hexdigest()[:8])

        path_ext = guess_url_file_extension(url)
        _L.debug(u'Guessed {}{} for {}'.format(name_base, path_ext, url))

        return os.path.join(dir_path, name_base + path_ext)

    def download(self, source_urls, workdir, conform=None):
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

            try:
                resp = request('GET', source_url, headers=self.headers, stream=True)
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
    def handle_esri_errors(self, response, error_message):
        if response.status_code != 200:
            raise DownloadError('{}: HTTP {} {}'.format(
                error_message,
                response.status_code,
                response.text,
            ))

        try:
            data = response.json()
        except:
            _L.error("Could not parse response from {} as JSON:\n\n{}".format(
                response.request.url,
                response.text,
            ))
            raise

        error = data.get('error')
        if error:
            raise DownloadError("{}: {} {}" .format(
                error_message,
                error['message'],
                ', '.join(error['details']),
            ))

        return data

    def build_ogr_geometry(self, geom_type, esri_feature):
        if 'geometry' not in esri_feature:
            raise TypeError("No geometry for feature")

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
            for esri_path in esri_feature['geometry']['paths']:
                line = ogr.Geometry(ogr.wkbLineString)
                for esri_pt in esri_path:
                    line.AddPoint(esri_pt[0], esri_pt[1])
                geom.AddGeometry(line)
        else:
            raise KeyError("Don't know how to convert esri geometry type {}".format(geom_type))

        return geom

    def get_file_path(self, url, dir_path):
        ''' Return a local file path in a directory for a URL.
        '''
        _, host, path, _, _, _ = urlparse(url)
        hash, path_ext = sha1((host + path).encode('utf-8')), '.csv'

        # With no source prefix like "us-ca-oakland" use the host as a hint.
        name_base = '{}-{}'.format(self.source_prefix or host, hash.hexdigest()[:8])

        _L.debug('Downloading {} to {}{}'.format(url, name_base, path_ext))

        return os.path.join(dir_path, name_base + path_ext)

    def get_layer_metadata(self, url):
        query_args = {
            'f': 'json'
        }
        response = request('GET', url, params=query_args, headers=self.headers)
        metadata = self.handle_esri_errors(response, "Could not retrieve field names from ESRI source")
        return metadata

    def get_layer_feature_count(self, url):
        query_args = {
            'where': '1=1',
            'returnCountOnly': 'true',
            'f': 'json',
        }
        response = request('GET', url, params=query_args, headers=self.headers)
        count_json = self.handle_esri_errors(response, "Could not retrieve row count from ESRI source")
        return count_json

    def get_layer_min_max(self, url, oid_field_name):
        """ Find the min and max values for the OID field. """
        query_args = {
            'f': 'json',
            'outFields': '',
            'outStatistics': json.dumps([
                dict(statisticType='min', onStatisticField=oid_field_name, outStatisticFieldName='THE_MIN'),
                dict(statisticType='max', onStatisticField=oid_field_name, outStatisticFieldName='THE_MAX'),
            ], separators=(',', ':'))
        }
        response = request('GET', url, params=query_args, headers=self.headers)
        metadata = self.handle_esri_errors(response, "Could not retrieve min/max oid values from ESRI source")

        # Some servers (specifically version 10.11, it seems) will respond with SQL statements
        # for the attribute names rather than the requested field names, so pick the min and max
        # deliberately rather than relying on the names.
        min_max_values = metadata['features'][0]['attributes'].values()
        return (min(min_max_values), max(min_max_values))

    def get_layer_oids(self, url):
        query_args = {
            'where': '1=1',  # So we get everything
            'returnIdsOnly': 'true',
            'f': 'json',
        }
        response = request('GET', url, params=query_args, headers=self.headers)
        oid_data = self.handle_esri_errors(response, "Could not retrieve object IDs from ESRI source")
        return oid_data

    def can_handle_pagination(self, query_url, query_fields):
        check_args = {
            'resultOffset': 0,
            'resultRecordCount': 1,
            'where': '1=1',
            'returnGeometry': 'false',
            'outFields': ','.join(query_fields),
            'f': 'json',
        }
        response = request('POST', query_url, headers=self.headers, data=check_args)

        try:
            data = response.json()
        except:
            _L.error("Could not parse response from pagination check {} as JSON:\n\n{}".format(
                response.request.url,
                response.text,
            ))
            return False

        return data.get('error') and data['error']['message'] != "Failed to execute query."

    def find_oid_field_name(self, metadata):
        oid_field_name = metadata.get('objectIdField')
        if not oid_field_name:
            for f in metadata['fields']:
                if f['type'] == 'esriFieldTypeOID':
                    oid_field_name = f['name']
                    break

        if not oid_field_name:
            raise DownloadError("Could not find object ID field name")

        return oid_field_name

    def field_names_to_request(self, conform):
        ''' Return list of fieldnames to request based on conform, or None.
        '''
        if not conform:
            return None

        fields = set()
        for k, v in conform.items():
            if k in attrib_types:
                if isinstance(v, dict):
                    # It's a function of some sort
                    fields.add(v.get('field'))
                elif isinstance(v, list):
                    # It's a list of field names
                    for f in v:
                        fields.add(f)
                else:
                    fields.add(v)

        if fields:
            return list(filter(None, sorted(fields)))
        else:
            return None

    def download(self, source_urls, workdir, conform=None):
        output_files = []
        download_path = os.path.join(workdir, 'esri')
        mkdirsp(download_path)

        query_fields = self.field_names_to_request(conform)

        for source_url in source_urls:
            size = 0
            file_path = self.get_file_path(source_url, download_path)

            if os.path.exists(file_path):
                output_files.append(file_path)
                _L.debug("File exists %s", file_path)
                continue

            metadata = self.get_layer_metadata(source_url)
            
            if query_fields is None:
                field_names = [f['name'] for f in metadata['fields']]
            else:
                field_names = query_fields[:]

            if X_FIELDNAME not in field_names:
                field_names.append(X_FIELDNAME)
            if Y_FIELDNAME not in field_names:
                field_names.append(Y_FIELDNAME)
            if GEOM_FIELDNAME not in field_names:
                field_names.append(GEOM_FIELDNAME)

            query_url = source_url + '/query'

            # Get the count of rows in the layer
            count_json = self.get_layer_feature_count(query_url)

            row_count = count_json.get('count')
            page_size = metadata.get('maxRecordCount', 500)
            if page_size > 1000:
                page_size = 1000

            _L.info("Source has {} rows".format(row_count))

            page_args = []

            if metadata.get('supportsPagination') or \
               (metadata.get('advancedQueryCapabilities') and metadata['advancedQueryCapabilities']['supportsPagination']):
                # If the layer supports pagination, we can use resultOffset/resultRecordCount to paginate

                # There's a bug where some servers won't handle these queries in combination with a list of
                # fields specified. We'll make a single, 1 row query here to check if the server supports this
                # and switch to querying for all fields if specifying the fields fails.
                if query_fields and not self.can_handle_pagination(query_url, query_fields):
                    _L.info("Source does not support pagination with fields specified, so querying for all fields.")
                    query_fields = None

                for offset in range(0, row_count, page_size):
                    page_args.append({
                        'resultOffset': offset,
                        'resultRecordCount': page_size,
                        'where': '1=1',
                        'geometryPrecision': 7,
                        'returnGeometry': 'true',
                        'outSR': 4326,
                        'outFields': ','.join(query_fields or ['*']),
                        'f': 'json',
                    })
                _L.info("Built {} requests using resultOffset method".format(len(page_args)))
            else:
                # If not, we can still use the `where` argument to paginate

                use_oids = True
                if metadata.get('supportsStatistics'):
                    # If the layer supports statistics, we can request maximum and minimum object ID
                    # to help build the pages

                    oid_field_name = self.find_oid_field_name(metadata)

                    try:
                        (oid_min, oid_max) = self.get_layer_min_max(query_url, oid_field_name)

                        for page_min in range(oid_min - 1, oid_max, page_size):
                            page_max = min(page_min + page_size, oid_max)
                            page_args.append({
                                'where': '{} > {} AND {} <= {}'.format(
                                    oid_field_name,
                                    page_min,
                                    oid_field_name,
                                    page_max,
                                ),
                                'geometryPrecision': 7,
                                'returnGeometry': 'true',
                                'outSR': 4326,
                                'outFields': ','.join(query_fields or ['*']),
                                'f': 'json',
                            })
                        _L.info("Built {} requests using OID where clause method".format(len(page_args)))

                        # If we reach this point we don't need to fall through to enumerating all object IDs
                        # because the statistics method worked
                        use_oids = False
                    except DownloadError:
                        _L.exception("Finding max/min from statistics failed. Trying OID enumeration.")

                if use_oids:
                    # If the layer does not support statistics, we can request
                    # all the individual IDs and page through them one chunk at
                    # a time.

                    oid_data = self.get_layer_oids(query_url)
                    oids = oid_data['objectIds']

                    for i in range(0, len(oids), 100):
                        oid_chunk = map(long if PY2 else int, oids[i:i+100])
                        page_args.append({
                            'objectIds': ','.join(map(str, oid_chunk)),
                            'geometryPrecision': 7,
                            'returnGeometry': 'true',
                            'outSR': 4326,
                            'outFields': ','.join(query_fields or ['*']),
                            'f': 'json',
                        })
                    _L.info("Built {} requests using OID enumeration method".format(len(page_args)))

            with csvopen(file_path, 'w', encoding='utf-8') as f:
                writer = csvDictWriter(f, fieldnames=field_names, encoding='utf-8')
                writer.writeheader()

                for query_args in page_args:
                    try:
                        response = request('POST', query_url, headers=self.headers, data=query_args)

                        data = self.handle_esri_errors(response, "Could not retrieve this chunk of objects from ESRI source")
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
                        raise DownloadError("Problem querying ESRI dataset with args {}. Server said: {}".format(query_args, error['message']))

                    geometry_type = data.get('geometryType')
                    features = data.get('features')

                    for feature in features:
                        try:
                            ogr_geom = self.build_ogr_geometry(geometry_type, feature)
                            row = feature.get('attributes', {})
                            row[GEOM_FIELDNAME] = ogr_geom.ExportToWkt()
                            try:
                                centroid = ogr_geom.Centroid()
                            except RuntimeError as e:
                                if 'Invalid number of points in LinearRing found' not in str(e):
                                    raise
                                xmin, xmax, ymin, ymax = ogr_geom.GetEnvelope()
                                row[X_FIELDNAME] = round(xmin/2 + xmax/2, 7)
                                row[Y_FIELDNAME] = round(ymin/2 + ymax/2, 7)
                            else:
                                row[X_FIELDNAME] = round(centroid.GetX(), 7)
                                row[Y_FIELDNAME] = round(centroid.GetY(), 7)

                            writer.writerow({fn: row.get(fn) for fn in field_names})
                            size += 1
                        except TypeError:
                            _L.debug("Skipping a geometry", exc_info=True)

            _L.info("Downloaded %s ESRI features for file %s", size, file_path)
            output_files.append(file_path)
        return output_files
