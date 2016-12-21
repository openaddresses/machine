from __future__ import absolute_import, division, print_function

from urllib.parse import urlparse, parse_qs
from os.path import join, dirname

import shutil
import mimetypes

from mock import patch
from esridump.errors import EsriDownloadError
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

    def test_download_with_conform(self):
        """ ESRI Caching Will Request With The Minimum Fields Required """
        conforms = (
            (None, None),
            (['a', 'b', 'c'], {'type': 'csv', 'street': ['a', 'b'], 'number': 'c'}),
            (['a'], {'type': 'csv', 'street': {'function': 'regexp', 'field': 'a'}, 'number': {'function': 'regexp', 'field': 'a'}}),
        )

        task = EsriRestDownloadTask('us-fl-palmbeach')
        for expected, conform in conforms:
            actual = task.field_names_to_request(conform)
            self.assertEqual(expected, actual)

    def test_download_handles_no_count(self):
        """ ESRI Caching Will Handle A Server Without returnCountOnly Support """
        task = EsriRestDownloadTask('us-fl-palmbeach')

        with patch('esridump.EsriDumper.get_metadata') as metadata_patch:
            metadata_patch.return_value = {'fields': []}
            with patch('esridump.EsriDumper.get_feature_count') as feature_patch:
                feature_patch.side_effect = EsriDownloadError("Server doesn't support returnCountOnly")
                with self.assertRaises(EsriDownloadError) as e:
                    task.download(['http://example.com/'], self.workdir)

                    # This is the expected exception at this point
                    self.assertEqual(e.message, "Could not find object ID field name for deduplication")

    def test_field_names_to_request(self):
        '''
        '''
        conform1 = dict(number='Number', street='Street')
        fields1 = EsriRestDownloadTask.field_names_to_request(conform1)
        self.assertEqual(fields1, ['Number', 'Street'])

        conform2 = dict(number='Number', street=dict(function='regexp', field='Street'))
        fields2 = EsriRestDownloadTask.field_names_to_request(conform2)
        self.assertEqual(fields2, ['Number', 'Street'])

        conform3 = dict(number='Number', street=dict(function='prefixed_number', field='Street'))
        fields3 = EsriRestDownloadTask.field_names_to_request(conform3)
        self.assertEqual(fields3, ['Number', 'Street'])

        conform4 = dict(number='Number', street=dict(function='postfixed_street', field='Street'))
        fields4 = EsriRestDownloadTask.field_names_to_request(conform4)
        self.assertEqual(fields4, ['Number', 'Street'])

        conform5 = dict(number='Number', street=dict(function='remove_prefix', field='Street'))
        fields5 = EsriRestDownloadTask.field_names_to_request(conform5)
        self.assertEqual(fields5, ['Number', 'Street'])

        conform6 = dict(number='Number', street=dict(function='remove_postfix', field='Street'))
        fields6 = EsriRestDownloadTask.field_names_to_request(conform6)
        self.assertEqual(fields6, ['Number', 'Street'])

        conform7 = dict(street=dict(function='join', fields=['Number', 'Street']))
        fields7 = EsriRestDownloadTask.field_names_to_request(conform7)
        self.assertEqual(fields7, ['Number', 'Street'])

        conform8 = dict(street=dict(function='format', fields=['Number', 'Street']))
        fields8 = EsriRestDownloadTask.field_names_to_request(conform8)
        self.assertEqual(fields8, ['Number', 'Street'])

        conform9 = dict(street=['Number', 'Street'])
        fields9 = EsriRestDownloadTask.field_names_to_request(conform9)
        self.assertEqual(fields9, ['Number', 'Street'])
