from base64 import b64encode
from datetime import datetime
from os.path import splitext, relpath
import unittest, json

from httmock import HTTMock, response
from uritemplate import expand

from ..ci.objects import Run
from ..summarize import (
    state_conform_type, is_coverage_complete, run_counts, convert_run
    )

class TestSummarizeFunctions (unittest.TestCase):

    def response_content(self, url, request):
        '''
        '''
        if (url.hostname, url.path) == ('example.com', '/sample.json'):
            return response(200, b'[["Yo"], [0]]', headers={'Content-Type': 'application/json; charset=utf-8'})
        
        raise ValueError()

    def test_state_conform_type(self):
        '''
        '''
        self.assertIsNone(state_conform_type({}))
        self.assertIsNone(state_conform_type({'cache': None}))

        self.assertEqual(state_conform_type({'cache': 'foo.zip', 'geometry type': 'Polygon'}),
                         'shapefile-polygon')

        self.assertEqual(state_conform_type({'cache': 'foo.zip', 'geometry type': 'MultiPolygon'}),
                         'shapefile-polygon')

        self.assertEqual(state_conform_type({'cache': 'foo.zip', 'geometry type': 'Point'}),
                         'shapefile')

        self.assertEqual(state_conform_type({'cache': 'foo.json'}), 'geojson')
        self.assertEqual(state_conform_type({'cache': 'foo.csv'}), 'csv')
        self.assertIsNone(state_conform_type({'cache': 'foo.bar'}))
    
    def test_is_coverage_complete(self):
        '''
        '''
        self.assertFalse(is_coverage_complete({}))
        self.assertFalse(is_coverage_complete({'coverage': 'Wat'}))
        self.assertFalse(is_coverage_complete({'coverage': {'Wat': None}}))
        self.assertTrue(is_coverage_complete({'coverage': {'ISO 3166': None}}))
        self.assertTrue(is_coverage_complete({'coverage': {'US Census': None}}))
        self.assertTrue(is_coverage_complete({'coverage': {'geometry': None}}))
    
    def test_run_counts(self):
        '''
        '''
        _ = None
        make_run = lambda state: Run(_, _, _, _, _, state, _, _, _, _, _, _, _)
        
        runs = [
            make_run({}),
            make_run({'cache': True}),
            make_run({'processed': True}),
            make_run({'cache': True, 'processed': True}),
            make_run({'address count': 1}),
            make_run({'cache': True, 'address count': 2}),
            make_run({'processed': True, 'address count': 4}),
            make_run({'cache': True, 'processed': True, 'address count': 8}),
            make_run({'address count': None}),
            make_run({'processed': False}),
            make_run({'cache': False}),
            ]
        
        counts = run_counts(runs)
        self.assertEqual(counts['sources'], 11)
        self.assertEqual(counts['cached'], 4)
        self.assertEqual(counts['processed'], 4)
        self.assertEqual(counts['addresses'], 15)
    
    def test_convert_run(self):
        '''
        '''
        source = {'conform': {}, 'skip': False, 'type': 'http'}
        source_b64 = b64encode(json.dumps(source).encode('utf8'))
        url_template = 'http://blob/{commit_sha}/{+source_path}'
        
        state = {'address count': 99, 'cache': 'zip1', 'cache time': '1:00',
                 'fingerprint': 'xyz', 'geometry type': 'Point', 'output': 'zip2',
                 'process time': '2:00', 'processed': 'zip3', 'version': '2015',
                 'sample': 'http://example.com/sample.json'}
        
        run = Run(id, 'sources/pl/foo.json', 'abc', source_b64, datetime.utcnow(),
                  state, True, None, '', '', None, None, 'def')
        
        with HTTMock(self.response_content):
            conv = convert_run(run, url_template)
        
        self.assertEqual(conv['address count'], state['address count'])
        self.assertEqual(conv['cache'], state['cache'])
        self.assertEqual(conv['cache time'], state['cache time'])
        self.assertEqual(conv['cache_date'], run.datetime_tz.strftime('%Y-%m-%d'))
        self.assertEqual(conv['conform'], bool(source['conform']))
        self.assertEqual(conv['conform type'], state_conform_type(state))
        self.assertEqual(conv['coverage complete'], is_coverage_complete(source))
        self.assertEqual(conv['fingerprint'], state['fingerprint'])
        self.assertEqual(conv['geometry type'], state['geometry type'])
        self.assertEqual(conv['href'], expand(url_template, run.__dict__))
        self.assertEqual(conv['output'], state['output'])
        self.assertEqual(conv['process time'], state['process time'])
        self.assertEqual(conv['processed'], state['processed'])
        self.assertEqual(conv['sample'], state['sample'])
        self.assertEqual(conv['sample_data'], [['Yo'], [0]])
        self.assertEqual(conv['shortname'], 'pl/foo')
        self.assertEqual(conv['skip'], source['skip'])
        self.assertEqual(conv['source'], 'pl/foo.json')
        self.assertEqual(conv['type'], source['type'])
        self.assertEqual(conv['version'], state['version'])
