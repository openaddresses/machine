# coding=utf8
from __future__ import print_function

from sys import stderr
from os import environ
from shutil import rmtree
from os.path import join
from tempfile import mkdtemp
from urllib.parse import parse_qsl
from zipfile import ZipFile
from datetime import date
import unittest

import mock
from httmock import HTTMock, response

from .. import LocalProcessedResult

from ..dotmap import (
    stream_all_features, call_tippecanoe, _upload_to_s3,
    _mapbox_get_credentials, _mapbox_create_upload
    )

class TestDotmap (unittest.TestCase):

    def setUp(self):
        self.test_dir = mkdtemp()
        self.results = list()
        
        self.results.append(LocalProcessedResult('us/anytown', join(self.test_dir, 'file1.zip'), None, None))
        zf = ZipFile(self.results[-1].filename, 'w')
        zf.writestr('README.txt', b'Good times')
        zf.writestr('stuff.csv', u'LAT,LON\n0,0\n37.804319,-122.271210\n'.encode('utf8'))
        zf.close()
        
        self.results.append(LocalProcessedResult('us/whoville', join(self.test_dir, 'file2.zip'), None, None))
        zf = ZipFile(self.results[-1].filename, 'w')
        zf.writestr('README.txt', b'Good times')
        zf.writestr('stuff.csv', u'LON,LAT,CITY\n0,0,Womp\n-122.413729,37.775641,Wómp Wómp\n'.encode('utf8'))
        zf.close()
    
    def tearDown(self):
        rmtree(self.test_dir)

    def test_stream_all_features_no_runs(self):
        features = list(stream_all_features(self.results[:0]))
        self.assertEqual(len(features), 0)

    def test_stream_all_features_one_run(self):
        features = list(stream_all_features(self.results[:1]))

        p1, p2 = [f['geometry']['coordinates'] for f in features]
        self.assertAlmostEqual(p1[0],    0.0)
        self.assertAlmostEqual(p1[1],    0.0)
        self.assertAlmostEqual(p2[0], -122.271210)
        self.assertAlmostEqual(p2[1],   37.804319)

    def test_stream_all_features_two_runs(self):
        features = list(stream_all_features(self.results[:2]))

        p1, p2, p3, p4 = [f['geometry']['coordinates'] for f in features]
        self.assertAlmostEqual(p1[0],    0.0)
        self.assertAlmostEqual(p1[1],    0.0)
        self.assertAlmostEqual(p2[0], -122.271210)
        self.assertAlmostEqual(p2[1],   37.804319)
        self.assertAlmostEqual(p3[0],    0.0)
        self.assertAlmostEqual(p3[1],    0.0)
        self.assertAlmostEqual(p4[0], -122.413729)
        self.assertAlmostEqual(p4[1],   37.775641)
    
    def test_call_tippecanoe(self):
        '''
        '''
        with mock.patch('subprocess.Popen') as Popen:
            call_tippecanoe('oa.mbtiles')
        
        self.assertEqual(len(Popen.mock_calls), 1)
        
        cmd = Popen.mock_calls[0][1][0]
        
        self.assertEqual('tippecanoe', cmd[0])
        self.assertEqual(('-o', 'oa.mbtiles'), cmd[-2:])
        self.assertIn('OpenAddresses {}'.format(str(date.today())), cmd)
    
    def response_content(self, url, request):
        '''
        '''
        query = dict(parse_qsl(url.query))
        MHP = request.method, url.hostname, url.path
        response_headers = {'Content-Type': 'application/json; charset=utf-8'}
        
        if MHP == ('GET', 'api.mapbox.com', '/uploads/v1/joe-blow/credentials') and query.get('access_token') == '0xDEADBEEF':
            data = '''{"bucket":"tilestream-tilesets-production","key":"_pending/joe-blow/cif4x7i1u03zcsakungpjhsxm","accessKeyId":"ASIAJTY66UOXKMZULHGQ","secretAccessKey":"1K0QxeZcVLxuatCM6HQxcPMncLDsyuvov3R0wANj","sessionToken":"AQoDYXdzEIf//////////wEakAN7iz5LVmkeitlx3vZ3uRdUKc9xgYviuTyQAh7Xcg905hhThTMMYSuxHIGGr/IdY0K6/hF543ys52BOgnFHV+ROKKk6jVXt3OIEjvEDi89zxzwWCvWxo1KncvgouHegzNXgxbzFEfGMydnLKwzdIT9nREOqWtZMIre7v6FNTQ21A3aHX7a6litzgwY6LQjFDs/0xCPNSiebD5Z6b3Gl6HzMC8D3lujqTA2nxkcSFxQ1iaiBSDkJRBrJN1a+4LvMyhS5NZcihBZYMYQCw1N4bLHh1tZuGb3bR8lKxh3ZBaDRCW3KrI+ER3qoob98PT1QvXvATwuuWn5k1doSjGBfbC309bfHItfi8cQB+ZwhhcDC7gJM52LCFEeoE4uvLqHzxqt1a2GLBTuCZ9JuEYCub06lplrkcgBOdyukqaMZlopHwQG+raXSfUY12Xqdw4vrnH90kW16YJ/TcJPwh8EK8gCQR+xvhX2hp7rWocFR9sIwv77gYTgU8YckizelzRcl4FDZV+79Jl3rpRuk5Hgy2aQdIMrBqLAF","url":"https://tilestream-tilesets-production.s3.amazonaws.com/_pending/joe-blow/cif4x7i1u03zcsakungpjhsxm"}'''
            return response(200, data.encode('utf8'), headers=response_headers)
        
        if MHP == ('POST', 'api.mapbox.com', '/uploads/v1/joe-blow') and query.get('access_token') == '0xDEADBEEF':
            data = '''{"complete": false, "created": "2015-09-27T23:20:25.520Z", "owner": "joe-blow", "error": null, "modified": "2015-09-27T23:20:25.520Z", "tileset": "oa.tiles", "progress": 0, "id": "0xARGLEBARGLE"}'''
            return response(200, data.encode('utf8'), headers=response_headers)
        
        if MHP == ('GET', 'api.mapbox.com', '/uploads/v1/joe-blow/0xARGLEBARGLE') and query.get('access_token') == '0xDEADBEEF':
            data = '''{"complete": true, "created": "2015-09-27T23:20:25.520Z", "owner": "joe-blow", "error": null, "modified": "2015-09-27T23:20:25.520Z", "tileset": "oa.tiles", "progress": 1, "id": "0xARGLEBARGLE"}'''
            return response(200, data.encode('utf8'), headers=response_headers)
        
        print('Unknowable Request {} "{}"'.format(request.method, url.geturl()), file=stderr)
        raise ValueError('Unknowable Request {} "{}"'.format(request.method, url.geturl()))
        
    def test_mapbox_get_credentials(self):
        with HTTMock(self.response_content):
            session_token, access_id, secret_key, bucket, s3_key, url \
                = _mapbox_get_credentials('joe-blow', '0xDEADBEEF')
            
            self.assertIn('joe-blow', s3_key)
            self.assertIn(s3_key, url)
    
    def test_upload_to_s3(self):
        environ['AWS_SESSION_TOKEN'] = 'Good Times'
        environ.pop('AWS_ACCESS_KEY_ID', None)
    
        with mock.patch('boto3.session.Session') as resource:
            _upload_to_s3('oa.mbtiles', 'xxx', 'yyy', 'zzz', 'bbb', 'kkk')
        
        self.assertEqual(len(resource.mock_calls), 4)
        self.assertEqual(resource.mock_calls[0][1], ('yyy', 'zzz', 'xxx'))
        self.assertEqual(resource.mock_calls[1][1], ('s3', ))
        self.assertEqual(resource.mock_calls[2][0], '().resource().Bucket')
        self.assertEqual(resource.mock_calls[2][1], ('bbb', ))
        self.assertEqual(resource.mock_calls[3][0], '().resource().Bucket().upload_file')
        self.assertEqual(resource.mock_calls[3][1], ('oa.mbtiles', 'kkk'))
            
        self.assertEqual(environ['AWS_SESSION_TOKEN'], 'Good Times')
        self.assertNotIn('AWS_ACCESS_KEY_ID', environ)
    
    def test_mapbox_create_upload(self):
        with HTTMock(self.response_content):
            _mapbox_create_upload('http://example.com/whatever', 'oa.tiles', 'joe-blow', '0xDEADBEEF')
