# Test suite. This code could be in a separate file

from shutil import rmtree
from os.path import dirname, join
from datetime import datetime

import unittest, tempfile, json
from mimetypes import guess_type
from urllib.parse import urlparse, parse_qs
from httmock import HTTMock, response
from mock import Mock

from ..compat import quote
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
    
    def test_task_instance(self):
        '''
        '''
        autoscale, ec2 = Mock(), Mock()
        group, config, image = Mock(), Mock(), Mock()
        keypair, reservation, instance = Mock(), Mock(), Mock()
        
        chef_role = 'good-times'
        command = 'openaddr-good-times', '--yo', 'b', 'd\\d', 'a"a', "s's", 'a:a'
        
        expected_group_name = 'CI Workers {0}.x'.format(*__version__.split('.'))
        expected_instance_name = 'Scheduled {} {}'.format(datetime.now().strftime('%Y-%m-%d'), command[0])
        
        autoscale.get_all_groups.return_value = [group]
        autoscale.get_all_launch_configurations.return_value = [config]
        ec2.get_all_images.return_value = [image]
        ec2.get_all_key_pairs.return_value = [keypair]
        
        image.run.return_value = reservation
        reservation.instances = [instance]
        
        util.request_task_instance(ec2, autoscale, chef_role, command)
        
        autoscale.get_all_groups.assert_called_once_with([expected_group_name])
        autoscale.get_all_launch_configurations.assert_called_once_with(names=[group.launch_config_name])
        ec2.get_all_images.assert_called_once_with(image_ids=[config.image_id])
        ec2.get_all_key_pairs.assert_called_once_with()
        
        image_run_kwargs = image.run.mock_calls[0][2]
        self.assertEqual(image_run_kwargs['instance_type'], 'm3.medium')
        self.assertEqual(image_run_kwargs['instance_initiated_shutdown_behavior'], 'terminate')
        self.assertEqual(image_run_kwargs['key_name'], keypair.name)
        
        self.assertIn('chef/run.sh {}'.format(quote(chef_role)), image_run_kwargs['user_data'])
        for arg in command:
            self.assertIn(quote(arg), image_run_kwargs['user_data'])
        
        instance.add_tag.assert_called_once_with('Name', expected_instance_name)

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
        
        if host == '96.31.228.112':
            if request.headers.get('Host') != 'www.carsonproperty.info':
                return response(404, 'Unknown whatever')
        
        if (host, path) == ('www.carsonproperty.info', '/ArcGIS/rest/services/basemap/MapServer/1/query') \
        or (host, path) == ('96.31.228.112', '/ArcGIS/rest/services/basemap/MapServer/1/query'):
            qs = parse_qs(query)
            body_data = parse_qs(request.body) if request.body else {}

            if 'token' in dict(qs):
                if 'nada' not in dict(qs)['token']:
                    return response(404, 'Bad token')
        
            if qs.get('returnIdsOnly') == ['true']:
                local_path = join(data_dirname, 'us-ca-carson-ids-only.json')
            elif qs.get('returnCountOnly') == ['true']:
                local_path = join(data_dirname, 'us-ca-carson-count-only.json')
            elif body_data.get('outSR') == ['4326']:
                local_path = join(data_dirname, 'us-ca-carson-0.json')

        if (host, path) == ('www.carsonproperty.info', '/ArcGIS/rest/services/basemap/MapServer/1') \
        or (host, path) == ('96.31.228.112', '/ArcGIS/rest/services/basemap/MapServer/1'):
            qs = parse_qs(query)

            if 'token' in dict(qs):
                if 'nada' not in dict(qs)['token']:
                    return response(404, 'Bad token')

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
    
    def test_conversion_extras(self):
    
        esri_url = 'http://96.31.228.112/ArcGIS/rest/services/basemap/MapServer/1'
        geojson_path = join(self.testdir, 'out.geojson')
        
        with HTTMock(self.response_content):
            esri2geojson(esri_url, geojson_path, params={'token': 'nada'},
                         headers={'Host': 'www.carsonproperty.info'})
        
        with open(geojson_path) as file:
            data = json.load(file)
        
        self.assertEqual(data['type'], 'FeatureCollection')
        self.assertEqual(len(data['features']), 5)

        self.assertEqual(data['features'][0]['type'], 'Feature')
        self.assertEqual(data['features'][0]['geometry']['type'], 'Point')
        self.assertEqual(data['features'][0]['properties']['ADDRESS'], '555 E CARSON ST 122')
