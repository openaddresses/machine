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
        memcache.get.return_value = b'\x80\x02}q\x00(U\x07conformq\x01\x89U\ncache_dateq\x02U\n2015-08-06q\x03U\x0bsample_dataq\x04]q\x05(]q\x06U\x02Yoq\x07a]q\x08K\x00aeU\x11coverage completeq\t\x89U\x04skipq\n\x89U\x0bfingerprintq\x0bU\x03xyzq\x0cU\x05cacheq\rU\x04zip1q\x0eU\x0cconform typeq\x0fNU\x06sampleq\x10U\x1ehttp://example.com/sample.jsonq\x11U\x06sourceq\x12U\x0bpl/foo.jsonq\x13U\x04hrefq\x14X#\x00\x00\x00http://blob/def/sources/pl/foo.jsonq\x15U\rgeometry typeq\x16U\x05Pointq\x17U\x07versionq\x18U\x042015q\x19U\ncache timeq\x1aU\x041:00q\x1bU\raddress countq\x1cKcU\x06outputq\x1dU\x04zip2q\x1eU\tshortnameq\x1fU\x06pl/fooq U\x04typeq!X\x04\x00\x00\x00httpq"U\x0cprocess timeq#U\x042:00q$U\tprocessedq%U\x04zip3q&u.'
        
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
