import json
import os
import errno
import socket
import mimetypes
import shutil

from logging import getLogger
from urllib import urlencode
from urlparse import urlparse
from hashlib import sha1

import requests

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


class URLDownloadTask(DownloadTask):
    USER_AGENT = 'openaddresses-extract/1.0 (https://github.com/openaddresses/openaddresses)'
    CHUNK = 16 * 1024

    logger = getLogger('openaddr')
    
    def get_file_path(self, url, dir_path):
        ''' Return a local file path in a directory for a URL.
        
            May need to fill in a filename extension based on HTTP Content-Type.
        '''
        _, host, path, _, _, _ = urlparse(url)
        path_base, path_ext = os.path.splitext(path)
        
        if self.source_prefix is None:
            # With no source prefix like "us-ca-oakland" use the name as given.
            name_base = os.path.basename(path_base)
        else:
            # With a source prefix, create a safe and unique filename with a hash.
            hash = sha1(host + path_base)
            name_base = '{}-{}'.format(self.source_prefix, hash.hexdigest()[:8])
        
        if not path_ext:
            resp = requests.head(url)
            path_ext = mimetypes.guess_extension(resp.headers['content-type'])
            self.logger.debug('Guessing {}{} for {}'.format(name_base, path_ext, resp.headers['content-type']))
        
        self.logger.debug('Downloading {} to {}{}'.format(path, name_base, path_ext))
        
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
                self.logger.debug("File exists %s", file_path)
                continue

            self.logger.debug("Requesting %s", source_url)
            headers = {'User-Agent': self.USER_AGENT}

            try:
                resp = requests.get(source_url, headers=headers, stream=True)
            except Exception, e:
                raise DownloadError("Could not connect to URL", e)

            size = 0
            with open(file_path, 'wb') as fp:
                for chunk in resp.iter_content(self.CHUNK):
                    size += len(chunk)
                    fp.write(chunk)

            output_files.append(file_path)

            self.logger.info("Downloaded %s bytes for file %s", size, file_path)

        return output_files


class EsriRestDownloadTask(DownloadTask):
    USER_AGENT = 'openaddresses-extract/1.0 (https://github.com/openaddresses/openaddresses)'

    logger = getLogger('openaddr')

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
        hash, path_ext = sha1(host + path), '.json'
        
        # With no source prefix like "us-ca-oakland" use the host as a hint.
        name_base = '{}-{}'.format(self.source_prefix or host, hash.hexdigest()[:8])
        
        self.logger.debug('Downloading {} to {}{}'.format(path, name_base, path_ext))
        
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
                self.logger.debug("File exists %s", file_path)
                continue

            with open(file_path, 'w') as f:
                f.write('{\n"type": "FeatureCollection",\n"features": [\n')
                start = 0
                width = 500
                while True:
                    query_url = source_url + '/query'
                    query_args = urlencode({
                        'where': 'objectid >= {} and objectid < {}'.format(start, (start + width)),
                        'geometryPrecision': 7,
                        'returnGeometry': True,
                        'outSR': 4326,
                        'outFields': '*',
                        'f': 'JSON',
                    })
                    query_url += '?' + query_args

                    self.logger.debug("Requesting %s", query_url)
                    headers = {'User-Agent': self.USER_AGENT}

                    try:
                        data = requests.get(query_url, headers=headers).json()
                    except socket.timeout as e:
                        raise DownloadError("Timeout when connecting to URL", e)
                    except ValueError as e:
                        raise DownloadError("Could not parse JSON", e)
                    except:
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
            self.logger.info("Downloaded %s ESRI features for file %s", size, file_path)
            output_files.append(file_path)
        return output_files
