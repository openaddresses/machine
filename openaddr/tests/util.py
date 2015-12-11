# Test suite. This code could be in a separate file

from shutil import rmtree
from os.path import dirname, join

import unittest, tempfile, json
from mimetypes import guess_type
from urllib.parse import urlparse, parse_qs
from httmock import HTTMock, response
from mock import Mock

from .. import util, __version__
from ..util.esri2geojson import esri2geojson

class TestUtilities (unittest.TestCase):

    def test_db_kwargs(self):
        '''
        '''
        dsn1 = 'postgres://who@where.kitchen/what'
        kwargs1 = util.prepare_db_kwargs(dsn1)
        self.assertEqual(kwargs1['user'], 'who')
        self.assertIsNone(kwargs1['password'])
        self.assertEqual(kwargs1['host'], 'where.kitchen')
        self.assertIsNone(kwargs1['port'])
        self.assertEqual(kwargs1['database'], 'what')
        self.assertNotIn('sslmode', kwargs1)

        dsn2 = 'postgres://who:open-sesame@where.kitchen:5432/what?sslmode=require'
        kwargs2 = util.prepare_db_kwargs(dsn2)
        self.assertEqual(kwargs2['user'], 'who')
        self.assertEqual(kwargs2['password'], 'open-sesame')
        self.assertEqual(kwargs2['host'], 'where.kitchen')
        self.assertEqual(kwargs2['port'], 5432)
        self.assertEqual(kwargs2['database'], 'what')
        self.assertEqual(kwargs2['sslmode'], 'require')
    
    def test_autoscale(self):
        '''
        '''
        autoscale, cloudwatch, as_group = Mock(), Mock(), Mock()
        group_name = 'CI Workers {0}.x'.format(*__version__.split('.'))
        
        cloudwatch.get_metric_statistics.return_value = [{}]
        autoscale.get_all_groups.return_value = [as_group]
        
        as_group.desired_capacity = 2
        util.set_autoscale_capacity(autoscale, cloudwatch, 1)
        
        # The right group name was used.
        autoscale.get_all_groups.assert_called_once_with([group_name])
        
        # Conditions haven't yet required a capacity increase.
        as_group.set_capacity.assert_not_called()

        as_group.desired_capacity = 1
        util.set_autoscale_capacity(autoscale, cloudwatch, 1)
        
        as_group.desired_capacity = 0
        cloudwatch.get_metric_statistics.return_value = [{'Maximum': 0}]
        util.set_autoscale_capacity(autoscale, cloudwatch, 1)
        
        cloudwatch.get_metric_statistics.return_value = [{'Maximum': 1}]
        util.set_autoscale_capacity(autoscale, cloudwatch, 1)
        
        # Capacity had to be increased to 1.
        as_group.set_capacity.assert_called_once_with(1)

        as_group.desired_capacity = 1
        util.set_autoscale_capacity(autoscale, cloudwatch, 2)
        
        # Capacity had to be increased to 2.
        as_group.set_capacity.assert_called_with(2)

class TestEsri2GeoJSON (unittest.TestCase):
    
    def setUp(self):
        ''' Prepare a clean temporary directory, and copy sources there.
        '''
        self.testdir = tempfile.mkdtemp(prefix='testEsri2GeoJSON-')
    
    def tearDown(self):
        rmtree(self.testdir)

    def response_content(self, url, request):
        ''' Fake HTTP responses for use with HTTMock in tests.
        '''
        scheme, host, path, _, query, _ = urlparse(url.geturl())
        data_dirname = join(dirname(__file__), 'data')
        local_path = None
        
        if (host, path) == ('www.carsonproperty.info', '/ArcGIS/rest/services/basemap/MapServer/1/query'):
            qs = parse_qs(query)
            body_data = parse_qs(request.body) if request.body else {}

            if qs.get('returnIdsOnly') == ['true']:
                local_path = join(data_dirname, 'us-ca-carson-ids-only.json')
            elif qs.get('returnCountOnly') == ['true']:
                local_path = join(data_dirname, 'us-ca-carson-count-only.json')
            elif body_data.get('outSR') == ['4326']:
                local_path = join(data_dirname, 'us-ca-carson-0.json')

        if (host, path) == ('www.carsonproperty.info', '/ArcGIS/rest/services/basemap/MapServer/1'):
            qs = parse_qs(query)

            if qs.get('f') == ['json']:
                local_path = join(data_dirname, 'us-ca-carson-metadata.json')

        if local_path:
            type, _ = guess_type(local_path)
            with open(local_path, 'rb') as file:
                return response(200, file.read(), headers={'Content-Type': type})
        
        raise NotImplementedError(url.geturl())
    
    def test_conversion(self):
    
        esri_url = 'http://www.carsonproperty.info/ArcGIS/rest/services/basemap/MapServer/1'
        geojson_path = join(self.testdir, 'out.geojson')
        
        with HTTMock(self.response_content):
            esri2geojson(esri_url, geojson_path)
        
        with open(geojson_path) as file:
            data = json.load(file)
        
        self.assertEqual(data['type'], 'FeatureCollection')
        self.assertEqual(len(data['features']), 5)

        self.assertEqual(data['features'][0]['type'], 'Feature')
        self.assertEqual(data['features'][0]['geometry']['type'], 'Point')
        self.assertEqual(data['features'][0]['properties']['ADDRESS'], '555 E CARSON ST 122')
