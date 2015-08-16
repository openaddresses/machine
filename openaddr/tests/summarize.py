# coding=utf8

from base64 import b64encode
from datetime import datetime
from os.path import splitext, relpath
import unittest, json

from httmock import HTTMock, response
import mock

from .. import __version__
from ..compat import expand_uri
from ..ci.objects import Run
from ..summarize import (
    state_conform_type, is_coverage_complete, run_counts, convert_run, summarize_set
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
        url_template = u'http://blob/{commit_sha}/{+source_path}'
        
        state = {'address count': 99, 'cache': 'zip1', 'cache time': '1:00',
                 'fingerprint': 'xyz', 'geometry type': 'Point', 'output': 'zip2',
                 'process time': '2:00', 'processed': 'zip3', 'version': '2015',
                 'sample': 'http://example.com/sample.json'}
        
        run = Run(456, u'sources/pl/foö.json', 'abc', source_b64, datetime.utcnow(),
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
        self.assertEqual(conv['href'], expand_uri(url_template, run.__dict__))
        self.assertEqual(conv['output'], state['output'])
        self.assertEqual(conv['process time'], state['process time'])
        self.assertEqual(conv['processed'], state['processed'])
        self.assertEqual(conv['sample'], state['sample'])
        self.assertEqual(conv['sample_data'], [['Yo'], [0]])
        self.assertEqual(conv['sample_link'], '/runs/456/sample.html')
        self.assertEqual(conv['shortname'], u'pl/foö')
        self.assertEqual(conv['skip'], source['skip'])
        self.assertEqual(conv['source'], u'pl/foö.json')
        self.assertEqual(conv['type'], source['type'])
        self.assertEqual(conv['version'], state['version'])
    
    def test_convert_run_cached(self):
        '''
        '''
        memcache = mock.Mock()
        memcache.get.return_value = b'\x80\x02}q\x00(X\x0b\x00\x00\x00fingerprintq\x01X\x03\x00\x00\x00xyzq\x02X\x04\x00\x00\x00typeq\x03X\x04\x00\x00\x00httpq\x04X\t\x00\x00\x00shortnameq\x05X\x07\x00\x00\x00pl/fo\xc3\xb6q\x06X\x06\x00\x00\x00sampleq\x07X\x1e\x00\x00\x00http://example.com/sample.jsonq\x08X\x07\x00\x00\x00versionq\tX\x04\x00\x00\x002015q\nX\x06\x00\x00\x00outputq\x0bX\x04\x00\x00\x00zip2q\x0cX\x0c\x00\x00\x00conform typeq\rNX\r\x00\x00\x00geometry typeq\x0eX\x05\x00\x00\x00Pointq\x0fX\n\x00\x00\x00cache_dateq\x10X\n\x00\x00\x002015-08-16q\x11X\x07\x00\x00\x00conformq\x12\x89X\x05\x00\x00\x00cacheq\x13X\x04\x00\x00\x00zip1q\x14X\r\x00\x00\x00address countq\x15KcX\x0b\x00\x00\x00sample_linkq\x16X\x15\x00\x00\x00/runs/456/sample.htmlq\x17X\x06\x00\x00\x00sourceq\x18X\x0c\x00\x00\x00pl/fo\xc3\xb6.jsonq\x19X\x04\x00\x00\x00hrefq\x1aX(\x00\x00\x00http://blob/def/sources/pl/fo%C3%B6.jsonq\x1bX\x0b\x00\x00\x00sample_dataq\x1c]q\x1d(]q\x1eX\x02\x00\x00\x00Yoq\x1fa]q K\x00aeX\x04\x00\x00\x00skipq!\x89X\t\x00\x00\x00processedq"X\x04\x00\x00\x00zip3q#X\n\x00\x00\x00cache timeq$X\x04\x00\x00\x001:00q%X\x0c\x00\x00\x00process timeq&X\x04\x00\x00\x002:00q\'X\x11\x00\x00\x00coverage completeq(\x89u.'
        
        source = {'conform': {}, 'skip': False, 'type': 'http'}
        source_b64 = b64encode(json.dumps(source).encode('utf8'))
        url_template = 'http://blob/{commit_sha}/{+source_path}'
        
        state = {'address count': 99, 'cache': 'zip1', 'cache time': '1:00',
                 'fingerprint': 'xyz', 'geometry type': 'Point', 'output': 'zip2',
                 'process time': '2:00', 'processed': 'zip3', 'version': '2015',
                 'sample': 'http://example.com/sample.json'}
        
        run = Run(456, u'sources/pl/foö.json', 'abc', source_b64, datetime.utcnow(),
                  state, True, None, '', '', None, None, 'def')
        
        with mock.patch('requests.get') as get:
            conv = convert_run(memcache, run, url_template)
            get.assert_not_called()
        
        memcache.set.assert_not_called()
        memcache.get.assert_called_once_with('converted-run-456-{}'.format(__version__))

        self.assertEqual(conv['address count'], state['address count'])
        self.assertEqual(conv['cache'], state['cache'])
        self.assertEqual(conv['cache time'], state['cache time'])
        self.assertEqual(conv['cache_date'], '2015-08-16', 'Should use a timestamp from the cached version')
        self.assertEqual(conv['conform'], bool(source['conform']))
        self.assertEqual(conv['conform type'], state_conform_type(state))
        self.assertEqual(conv['coverage complete'], is_coverage_complete(source))
        self.assertEqual(conv['fingerprint'], state['fingerprint'])
        self.assertEqual(conv['geometry type'], state['geometry type'])
        self.assertEqual(conv['href'], expand_uri(url_template, run.__dict__))
        self.assertEqual(conv['output'], state['output'])
        self.assertEqual(conv['process time'], state['process time'])
        self.assertEqual(conv['processed'], state['processed'])
        self.assertEqual(conv['sample'], state['sample'])
        self.assertEqual(conv['sample_data'], [['Yo'], [0]])
        self.assertEqual(conv['sample_link'], '/runs/456/sample.html')
        self.assertEqual(conv['shortname'], u'pl/foö')
        self.assertEqual(conv['skip'], source['skip'])
        self.assertEqual(conv['source'], u'pl/foö.json')
        self.assertEqual(conv['type'], source['type'])
        self.assertEqual(conv['version'], state['version'])
    
    def test_summarize_set(self):
        '''
        '''
        memcache, set, run = mock.Mock(), mock.Mock(), mock.Mock()
        set.owner, set.repository = u'oa', u'oa'
        
        with mock.patch('openaddr.summarize.convert_run') as convert_run, \
             mock.patch('openaddr.summarize.run_counts') as run_counts:
            convert_run.return_value = {
                'cache': 'zip1', 'processed': 'zip2', 'source': 'foo'
                }
            run_counts.return_value = {'sources': 1, 'cached': 1, 'processed': 1}
        
            summary_html = summarize_set(memcache, set, [run])
