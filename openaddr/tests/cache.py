from __future__ import absolute_import, division, print_function

from urllib.parse import urlparse, parse_qs
from os.path import join, dirname

import shutil
import mimetypes

import unittest
import httmock
import tempfile

from ..cache import guess_url_file_extension, EsriRestDownloadTask

class TestCacheExtensionGuessing (unittest.TestCase):

    def response_content(self, url, request):
        ''' Fake HTTP responses for use with HTTMock in tests.
        '''
        scheme, host, path, _, query, _ = urlparse(url.geturl())
        tests_dirname = dirname(__file__)
        
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

        elif (host, path, query) == ('data.northcowichan.ca', '/DataBrowser/DownloadCsv', 'container=mncowichan&entitySet=PropertyReport&filter=NOFILTER'):
            return httmock.response(200, b'FAKE,FAKE\n'*99, headers={'Content-Type': 'text/csv', 'Content-Disposition': 'attachment; filename=PropertyReport.csv'})

        raise NotImplementedError(url.geturl())
    
    def test_urls(self):
        with httmock.HTTMock(self.response_content):
            assert guess_url_file_extension('http://fake-cwd.local/conforms/lake-man-3740.csv') == '.csv'
            assert guess_url_file_extension('http://fake-cwd.local/data/us-ca-carson-0.json') == '.json'
            assert guess_url_file_extension('http://fake-cwd.local/data/us-ca-oakland-excerpt.zip') == '.zip'
            assert guess_url_file_extension('http://www.ci.berkeley.ca.us/uploadedFiles/IT/GIS/Parcels.zip') == '.zip'
            assert guess_url_file_extension('https://data.sfgov.org/download/kvej-w5kb/ZIPPED%20SHAPEFILE') == '.zip'
            assert guess_url_file_extension('http://dcatlas.dcgis.dc.gov/catalog/download.asp?downloadID=2182&downloadTYPE=ESRI') == '.zip'
            assert guess_url_file_extension('http://data.northcowichan.ca/DataBrowser/DownloadCsv?container=mncowichan&entitySet=PropertyReport&filter=NOFILTER') == '.csv', guess_url_file_extension('http://data.northcowichan.ca/DataBrowser/DownloadCsv?container=mncowichan&entitySet=PropertyReport&filter=NOFILTER')

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
        data_dirname = join(dirname(__file__), 'data')
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
