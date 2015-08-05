from base64 import b64encode
from datetime import datetime
from os.path import splitext, relpath
import unittest, json

from httmock import HTTMock, response
from uritemplate import expand
import mock

from .. import __version__
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
        make_run = lambda state: Run(_, _, _, b'', _, state, _, _, _, _, _, _, _)
        
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
    
    def test_convert_run_uncached(self):
        '''
        '''
        memcache = mock.Mock()
        memcache.get.return_value = None
        
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
            conv = convert_run(memcache, run, url_template)
        
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
    
    def test_convert_run_cached(self):
        '''
        '''
        memcache = mock.Mock()
        memcache.get.return_value = b'\x80\x03}q\x00(X\t\x00\x00\x00shortnameq\x01X\x06\x00\x00\x00pl/fooq\x02X\t\x00\x00\x00processedq\x03X\x04\x00\x00\x00zip3q\x04X\r\x00\x00\x00address countq\x05KcX\x04\x00\x00\x00typeq\x06X\x04\x00\x00\x00httpq\x07X\n\x00\x00\x00cache_dateq\x08X\n\x00\x00\x002015-08-05q\tX\x06\x00\x00\x00sourceq\nX\x0b\x00\x00\x00pl/foo.jsonq\x0bX\x07\x00\x00\x00versionq\x0cX\x04\x00\x00\x002015q\rX\x11\x00\x00\x00coverage completeq\x0e\x89X\x06\x00\x00\x00sampleq\x0fX\x1e\x00\x00\x00http://example.com/sample.jsonq\x10X\x04\x00\x00\x00skipq\x11\x89X\x04\x00\x00\x00hrefq\x12X#\x00\x00\x00http://blob/def/sources/pl/foo.jsonq\x13X\x07\x00\x00\x00conformq\x14\x89X\x0b\x00\x00\x00fingerprintq\x15X\x03\x00\x00\x00xyzq\x16X\r\x00\x00\x00geometry typeq\x17X\x05\x00\x00\x00Pointq\x18X\x0c\x00\x00\x00process timeq\x19X\x04\x00\x00\x002:00q\x1aX\x05\x00\x00\x00cacheq\x1bX\x04\x00\x00\x00zip1q\x1cX\x06\x00\x00\x00outputq\x1dX\x04\x00\x00\x00zip2q\x1eX\x0b\x00\x00\x00sample_dataq\x1f]q (]q!X\x02\x00\x00\x00Yoq"a]q#K\x00aeX\n\x00\x00\x00cache timeq$X\x04\x00\x00\x001:00q%X\x0c\x00\x00\x00conform typeq&Nu.'
        
        source = {'conform': {}, 'skip': False, 'type': 'http'}
        source_b64 = b64encode(json.dumps(source).encode('utf8'))
        url_template = 'http://blob/{commit_sha}/{+source_path}'
        
        state = {'address count': 99, 'cache': 'zip1', 'cache time': '1:00',
                 'fingerprint': 'xyz', 'geometry type': 'Point', 'output': 'zip2',
                 'process time': '2:00', 'processed': 'zip3', 'version': '2015',
                 'sample': 'http://example.com/sample.json'}
        
        run = Run(456, 'sources/pl/foo.json', 'abc', source_b64, datetime.utcnow(),
                  state, True, None, '', '', None, None, 'def')
        
        with mock.patch('requests.get') as get:
            conv = convert_run(memcache, run, url_template)
            get.assert_not_called()
        
        memcache.set.assert_not_called()
        memcache.get.assert_called_once_with('converted-run-456-{}'.format(__version__))

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
