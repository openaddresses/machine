"""
Run Python test suite via the standard unittest mechanism.
Usage:
  python test.py
  python test.py --logall
  python test.py TestConformTransforms
  python test.py -l TestOA.test_process
All logging is suppressed unless --logall or -l specified
~/.openaddr-logging-test.json can also be used to configure log behavior
"""


from __future__ import absolute_import, division, print_function
from future import standard_library; standard_library.install_aliases()

import unittest
import shutil
import tempfile
import json
import re
import sys
import pickle
import logging
from os import close, environ, mkdir, remove
from io import BytesIO
from mimetypes import guess_type
from urllib.parse import urlparse, parse_qs
from os.path import dirname, join, basename, exists
from fcntl import lockf, LOCK_EX, LOCK_UN
from contextlib import contextmanager
from subprocess import Popen, PIPE
from csv import DictReader
from threading import Lock

from requests import get
from httmock import response, HTTMock
        
from openaddr import paths, cache, conform, jobs, S3, process_all, process_one
from openaddr.sample import TestSample
from openaddr.cache import TestCacheExtensionGuessing
from openaddr.conform import TestConformCli, TestConformTransforms, TestConformMisc, TestConformCsv
from openaddr.expand import TestExpand

class TestOA (unittest.TestCase):
    
    def setUp(self):
        ''' Prepare a clean temporary directory, and copy sources there.
        '''
        self.testdir = tempfile.mkdtemp(prefix='testOA-')
        self.src_dir = join(self.testdir, 'sources')
        sources_dir = join(dirname(__file__), 'tests', 'sources')
        shutil.copytree(sources_dir, self.src_dir)

        self.s3 = FakeS3()
    
    def tearDown(self):
        shutil.rmtree(self.testdir)
        remove(self.s3._fake_keys)

    def response_content(self, url, request):
        ''' Fake HTTP responses for use with HTTMock in tests.
        '''
        scheme, host, path, _, query, _ = urlparse(url.geturl())
        data_dirname = join(dirname(__file__), 'tests', 'data')
        local_path = None
        
        if host == 'fake-s3.local':
            return response(200, self.s3._read_fake_key(path))
        
        if (host, path) == ('data.acgov.org', '/api/geospatial/8e4s-7f4v'):
            local_path = join(data_dirname, 'us-ca-alameda_county-excerpt.zip')
        
        if (host, path) == ('www.ci.berkeley.ca.us', '/uploadedFiles/IT/GIS/Parcels.zip'):
            local_path = join(data_dirname, 'us-ca-berkeley-excerpt.zip')
        
        if (host, path) == ('www.ci.berkeley.ca.us', '/uploadedFiles/IT/GIS/No-Parcels.zip'):
            return response(404, 'Nobody here but us coats')
        
        if (host, path) == ('data.openoakland.org', '/sites/default/files/OakParcelsGeo2013_0.zip'):
            local_path = join(data_dirname, 'us-ca-oakland-excerpt.zip')
        
        if (host, path) == ('data.sfgov.org', '/download/kvej-w5kb/ZIPPED%20SHAPEFILE'):
            local_path = join(data_dirname, 'us-ca-san_francisco-excerpt.zip')
        
        if (host, path) == ('www.carsonproperty.info', '/ArcGIS/rest/services/basemap/MapServer/1/query'):
            qs = parse_qs(query)
            body_data = parse_qs(request.body) if request.body else {}

            if qs.get('returnIdsOnly') == ['true']:
                local_path = join(data_dirname, 'us-ca-carson-ids-only.json')
            elif body_data.get('outSR') == ['4326']:
                local_path = join(data_dirname, 'us-ca-carson-0.json')

        if (host, path) == ('www.carsonproperty.info', '/ArcGIS/rest/services/basemap/MapServer/1'):
            qs = parse_qs(query)

            if qs.get('f') == ['json']:
                local_path = join(data_dirname, 'us-ca-carson-metadata.json')

        if (host, path) == ('data.openaddresses.io', '/20000101/us-ca-carson-cached.json'):
            local_path = join(data_dirname, 'us-ca-carson-cache.geojson')
        
        if scheme == 'file':
            local_path = path

        if local_path:
            type, _ = guess_type(local_path)
            with open(local_path) as file:
                return response(200, file.read(), headers={'Content-Type': type})
        
        raise NotImplementedError(url.geturl())
    
    def test_process_all(self):
        ''' Test process_all.process(), with complete threaded behavior.
        '''
        with HTTMock(self.response_content):
            process_all.process(self.s3, self.src_dir, 'test')
        
        # Go looking for state.txt in fake S3.
        buffer = BytesIO(self.s3._read_fake_key('runs/test/state.txt'))
        states = dict([(row['source'], row) for row
                       in DictReader(buffer, dialect='excel-tab')])
        
        for (source, state) in states.items():
            if 'berkeley-404' in source or 'oakland-skip' in source:
                self.assertFalse(bool(state['cache']), 'Checking for cache in {}'.format(source))
                self.assertFalse(bool(state['version']), 'Checking for version in {}'.format(source))
                self.assertFalse(bool(state['fingerprint']), 'Checking for fingerprint in {}'.format(source))
            else:
                self.assertTrue(bool(state['cache']), 'Checking for cache in {}'.format(source))
                self.assertTrue(bool(state['version']), 'Checking for version in {}'.format(source))
                self.assertTrue(bool(state['fingerprint']), 'Checking for fingerprint in {}'.format(source))
            
                self.assertTrue(bool(state['geometry type']), 'Checking for geometry type in {}'.format(source))
                self.assertTrue(bool(state['sample']), 'Checking for sample in {}'.format(source))

            if 'san_francisco' in source or 'alameda_county' in source or 'carson' in source:
                self.assertTrue(bool(state['processed']), "Checking for processed in {}".format(source))
            else:
                self.assertFalse(bool(state['processed']), "Checking for processed in {}".format(source))

        #
        # Check the JSON version of the data.
        #
        data = json.loads(self.s3._read_fake_key('state.json'))
        self.assertEqual(data, 'runs/test/state.json')
        
        data = json.loads(self.s3._read_fake_key(data))
        rows = [dict(zip(data[0], row)) for row in data[1:]]
        
        for state in rows:
            if 'berkeley-404' in state['source'] or 'oakland-skip' in state['source']:
                self.assertFalse(bool(state['cache']))
                self.assertFalse(bool(state['version']))
                self.assertFalse(bool(state['fingerprint']))
            else:
                self.assertTrue(bool(state['cache']))
                self.assertTrue(bool(state['version']))
                self.assertTrue(bool(state['fingerprint']))
        
    def test_single_ac(self):
        ''' Test complete process_one.process on Alameda County sample data.
        '''
        source = join(self.src_dir, 'us-ca-alameda_county.json')
        
        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir)
        
        with open(state_path) as file:
            state = dict(zip(*json.load(file)))
        
        self.assertTrue(state['cache'] is not None)
        self.assertTrue(state['processed'] is not None)
        self.assertTrue(state['sample'] is not None)
        self.assertEqual(state['geometry type'], 'Point')
        
        with open(join(dirname(state_path), state['sample'])) as file:
            sample_data = json.load(file)
        
        self.assertEqual(len(sample_data), 6)
        self.assertTrue('ZIPCODE' in sample_data[0])
        self.assertTrue('OAKLAND' in sample_data[1])
        self.assertTrue('94612' in sample_data[1])

    def test_single_car(self):
        ''' Test complete process_one.process on Carson sample data.
        '''
        source = join(self.src_dir, 'us-ca-carson.json')
        
        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir)
        
        with open(state_path) as file:
            state = dict(zip(*json.load(file)))
        
        self.assertTrue(state['cache'] is not None)
        self.assertEqual(state['fingerprint'], 'b548d1f9f1e19824a90d456e90518991')
        self.assertTrue(state['processed'] is not None)
        self.assertTrue(state['sample'] is not None)
        
        with open(join(dirname(state_path), state['sample'])) as file:
            sample_data = json.load(file)
        
        self.assertEqual(len(sample_data), 6)
        self.assertTrue('SITEFRAC' in sample_data[0])
        
        with open(join(dirname(state_path), state['processed'])) as file:
            self.assertTrue('555,Carson Street' in file.read())

    def test_single_car_cached(self):
        ''' Test complete process_one.process on Carson sample data.
        '''
        source = join(self.src_dir, 'us-ca-carson-cached.json')
        
        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir)
        
        with open(state_path) as file:
            state = dict(zip(*json.load(file)))
        
        self.assertTrue(state['cache'] is not None)
        self.assertEqual(state['fingerprint'], 'b548d1f9f1e19824a90d456e90518991')
        self.assertTrue(state['processed'] is not None)
        self.assertTrue(state['sample'] is not None)
        
        with open(join(dirname(state_path), state['sample'])) as file:
            sample_data = json.load(file)
        
        self.assertEqual(len(sample_data), 6)
        self.assertTrue('SITEFRAC' in sample_data[0])
        
        with open(join(dirname(state_path), state['processed'])) as file:
            self.assertTrue('555,Carson Street' in file.read())

    def test_single_car_old_cached(self):
        ''' Test complete process_one.process on Carson sample data.
        '''
        source = join(self.src_dir, 'us-ca-carson-old-cached.json')
        
        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir)
        
        with open(state_path) as file:
            state = dict(zip(*json.load(file)))
        
        self.assertTrue(state['cache'] is not None)
        self.assertEqual(state['fingerprint'], 'b548d1f9f1e19824a90d456e90518991')
        self.assertTrue(state['processed'] is not None)
        self.assertTrue(state['sample'] is not None)
        
        with open(join(dirname(state_path), state['sample'])) as file:
            sample_data = json.load(file)
        
        self.assertEqual(len(sample_data), 6)
        self.assertTrue('SITEFRAC' in sample_data[0])
        
        with open(join(dirname(state_path), state['processed'])) as file:
            self.assertTrue('555,Carson Street' in file.read())

    def test_single_oak(self):
        ''' Test complete process_one.process on Oakland sample data.
        '''
        source = join(self.src_dir, 'us-ca-oakland.json')
        
        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir)
        
        with open(state_path) as file:
            state = dict(zip(*json.load(file)))
        
        self.assertTrue(state['cache'] is not None)
        # This test data does not contain a working conform object
        self.assertTrue(state['processed'] is None)
        
        with open(join(dirname(state_path), state['sample'])) as file:
            sample_data = json.load(file)
        
        self.assertTrue('FID_PARCEL' in sample_data[0])

    def test_single_oak_skip(self):
        ''' Test complete process_one.process on Oakland sample data.
        '''
        source = join(self.src_dir, 'us-ca-oakland-skip.json')
        
        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir)
        
        with open(state_path) as file:
            state = dict(zip(*json.load(file)))
        
        self.assertTrue(state['cache'] is None)
        # This test data does not contain a working conform object
        self.assertTrue(state['processed'] is None)

    def test_single_berk(self):
        ''' Test complete process_one.process on Berkeley sample data.
        '''
        source = join(self.src_dir, 'us-ca-berkeley.json')
        
        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir)
        
        with open(state_path) as file:
            state = dict(zip(*json.load(file)))
        
        self.assertTrue(state['cache'] is not None)
        # This test data does not contain a conform object at all
        self.assertTrue(state['processed'] is None)
        
        with open(join(dirname(state_path), state['sample'])) as file:
            sample_data = json.load(file)
        
        self.assertTrue('APN' in sample_data[0])

    def test_single_berk_404(self):
        ''' Test complete process_one.process on 404 sample data.
        '''
        source = join(self.src_dir, 'us-ca-berkeley-404.json')
        
        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir)
        
        with open(state_path) as file:
            state = dict(zip(*json.load(file)))
        
        self.assertTrue(state['cache'] is None)
        self.assertTrue(state['processed'] is None)

@contextmanager
def locked_open(filename):
    ''' Open and lock a file, for use with threads and processes.
    '''
    with open(filename, 'r+b') as file:
        lockf(file, LOCK_EX)
        yield file
        lockf(file, LOCK_UN)

class FakeS3 (S3):
    ''' Just enough S3 to work for tests.
    '''
    _fake_keys = None
    
    def __init__(self):
        handle, self._fake_keys = tempfile.mkstemp(prefix='fakeS3-', suffix='.pickle')
        close(handle)

        self._threadlock = Lock()
        
        with open(self._fake_keys, 'wb') as file:
            pickle.dump(dict(), file)

        S3.__init__(self, 'Fake Key', 'Fake Secret', 'data-test.openaddresses.io')
    
    def _write_fake_key(self, name, string):
        with locked_open(self._fake_keys) as file, self._threadlock:
            data = pickle.load(file)
            data[name] = string
            
            file.seek(0)
            file.truncate()
            pickle.dump(data, file)
    
    def _read_fake_key(self, name):
        with locked_open(self._fake_keys) as file, self._threadlock:
            data = pickle.load(file)
            
        return data[name]
    
    def get_key(self, name):
        if not name.endswith('state.txt'):
            raise NotImplementedError()
        # No pre-existing state for testing.
        return None
        
    def new_key(self, name):
        return FakeKey(name, self)

class FakeKey:
    ''' Just enough S3 to work for tests.
    '''
    md5 = '0xDEADBEEF'
    
    def __init__(self, name, fake_s3):
        self.name = name
        self.s3 = fake_s3
    
    def generate_url(self, **kwargs):
        if kwargs.get('force_http', None) is not True:
            raise ValueError("S3 generate_url() makes bad https:// URLs")
    
        return 'http://fake-s3.local' + self.name

    def set_contents_from_string(self, string, **kwargs):
        self.s3._write_fake_key(self.name, string)
        
    def set_contents_from_filename(self, filename, **kwargs):
        with open(filename) as file:
            self.s3._write_fake_key(self.name, file.read())

if __name__ == '__main__':
    # Allow the user to turn on logging with -l or --logall
    # unittest.main() has its own command line so we slide this in first
    level = logging.CRITICAL
    for i, arg in enumerate(sys.argv[1:]):
        if arg == "-l" or arg == "--logall":
            level = logging.DEBUG
            del sys.argv[i]

    jobs.setup_logger(log_level = level, log_config_file = "~/.openaddr-logging-test.json")
    unittest.main()
