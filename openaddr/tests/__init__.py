# coding=utf8
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
from ..compat import standard_library, csvDictReader

import unittest
import shutil
import tempfile
import json
import re
import pickle
from os import close, environ, mkdir, remove
from io import BytesIO
from zipfile import ZipFile
from mimetypes import guess_type
from urllib.parse import urlparse, parse_qs
from os.path import dirname, join, basename, exists, splitext
from fcntl import lockf, LOCK_EX, LOCK_UN
from contextlib import contextmanager
from subprocess import Popen, PIPE
from threading import Lock

from requests import get
from httmock import response, HTTMock
        
from .. import paths, cache, conform, S3, process_all, process_one

class TestOA (unittest.TestCase):
    
    def setUp(self):
        ''' Prepare a clean temporary directory, and copy sources there.
        '''
        self.testdir = tempfile.mkdtemp(prefix='testOA-')
        self.src_dir = join(self.testdir, 'sources')
        sources_dir = join(dirname(__file__), 'sources')
        shutil.copytree(sources_dir, self.src_dir)

        self.s3 = FakeS3()
    
    def tearDown(self):
        shutil.rmtree(self.testdir)
        remove(self.s3._fake_keys)

    def response_content(self, url, request):
        ''' Fake HTTP responses for use with HTTMock in tests.
        '''
        scheme, host, path, _, query, _ = urlparse(url.geturl())
        data_dirname = join(dirname(__file__), 'data')
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
        
        if (host, path) == ('s3.amazonaws.com', '/data.openaddresses.io/cache/pl.zip'):
            local_path = join(data_dirname, 'pl.zip')
        
        if (host, path) == ('s3.amazonaws.com', '/data.openaddresses.io/cache/jp-fukushima.zip'):
            local_path = join(data_dirname, 'jp-fukushima.zip')
        
        if (host, path) == ('data.sfgov.org', '/download/kvej-w5kb/ZIPPED%20SHAPEFILE'):
            local_path = join(data_dirname, 'us-ca-san_francisco-excerpt.zip')

        if (host, path) == ('ftp.agrc.utah.gov', '/UtahSGID_Vector/UTM12_NAD83/LOCATION/UnpackagedData/AddressPoints/_Statewide/AddressPoints_shp.zip'):
            local_path = join(data_dirname, 'us-ut-excerpt.zip')
        
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
            with open(local_path, 'rb') as file:
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
                       in csvDictReader(buffer, dialect='excel-tab')])
        
        for (source, state) in states.items():
            if 'berkeley-404' in source or 'oakland-skip' in source:
                self.assertFalse(bool(state['cache']), 'Checking for cache in {}'.format(source))
                self.assertFalse(bool(state['version']), 'Checking for version in {}'.format(source))
                self.assertFalse(bool(state['fingerprint']), 'Checking for fingerprint in {}'.format(source))
            else:
                self.assertTrue(bool(state['cache']), 'Checking for cache in {}'.format(source))
                self.assertTrue(bool(state['version']), 'Checking for version in {}'.format(source))
                self.assertTrue(bool(state['fingerprint']), 'Checking for fingerprint in {}'.format(source))
            
                self.assertTrue(bool(state['sample']), 'Checking for sample in {}'.format(source))

            if 'san_francisco' in source or 'alameda_county' in source or 'carson' in source or 'pl-' in source or 'us-ut' in source:
                self.assertTrue(bool(state['processed']), "Checking for processed in {}".format(source))
                
                with HTTMock(self.response_content):
                    got = get(state['processed'])
                    zip_file = ZipFile(BytesIO(got.content), mode='r')
                
                source_base, _ = splitext(source)
                self.assertTrue(source_base + '.csv' in zip_file.namelist())
                self.assertTrue(source_base + '.vrt' in zip_file.namelist())
                
            else:
                self.assertFalse(bool(state['processed']), "Checking for processed in {}".format(source))
            
            if 'berkeley-404' in source or 'oakland-skip' in source:
                self.assertFalse(bool(state['geometry type']))
            elif 'berkeley' in source or 'oakland' in source:
                self.assertEqual(state['geometry type'], 'Polygon')
            elif 'san_francisco' in source or 'alameda_county' in source:
                self.assertEqual(state['geometry type'], 'Point')
            elif 'carson' in source:
                self.assertEqual(state['geometry type'], 'Point 2.5D')

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
        
        #
        # Check for a zip with everything.
        #
        bytes = BytesIO(self.s3._read_fake_key('openaddresses-complete.zip'))
        names = ZipFile(bytes).namelist()
        
        for state in rows:
            source = state['source']
            if 'san_francisco' in source or 'alameda_county' in source or 'carson' in source:
                name = '{0}.csv'.format(*splitext(state['source']))
                self.assertTrue(name in names, 'Looking for {} in zip'.format(name))
        
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
        self.assertEqual(state['fingerprint'], '3926017394c9ff4d6a68718a0a503620')
        self.assertTrue(state['processed'] is not None)
        self.assertTrue(state['sample'] is not None)
        self.assertEqual(state['geometry type'], 'Point 2.5D')
        
        with open(join(dirname(state_path), state['sample'])) as file:
            sample_data = json.load(file)
        
        self.assertEqual(len(sample_data), 6)
        self.assertTrue('SITEFRAC' in sample_data[0])
        
        with open(join(dirname(state_path), state['processed'])) as file:
            data = file.read().strip()
            self.assertEqual(6, len(data.split('\n')))
            self.assertTrue('555,Carson Street' in data)

    def test_single_car_cached(self):
        ''' Test complete process_one.process on Carson sample data.
        '''
        source = join(self.src_dir, 'us-ca-carson-cached.json')
        
        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir)
        
        with open(state_path) as file:
            state = dict(zip(*json.load(file)))
        
        self.assertTrue(state['cache'] is not None)
        self.assertEqual(state['fingerprint'], '3926017394c9ff4d6a68718a0a503620')
        self.assertTrue(state['processed'] is not None)
        self.assertTrue(state['sample'] is not None)
        self.assertEqual(state['geometry type'], 'Point 2.5D')
        
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
        self.assertEqual(state['fingerprint'], '3926017394c9ff4d6a68718a0a503620')
        self.assertTrue(state['processed'] is not None)
        self.assertTrue(state['sample'] is not None)
        self.assertEqual(state['geometry type'], 'Point 2.5D')
        
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
        
    def test_single_pl_ds(self):
        ''' Test complete process_one.process on Polish sample data.
        '''
        source = join(self.src_dir, 'pl-dolnoslaskie.json')
        
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
        self.assertTrue('pad_numer_porzadkowy' in sample_data[0])
        self.assertTrue(u'Wrocław' in sample_data[1])
        self.assertTrue(u'Ulica Księcia Witolda ' in sample_data[1])
        
    def test_single_pl_l(self):
        ''' Test complete process_one.process on Polish sample data.
        '''
        source = join(self.src_dir, 'pl-lodzkie.json')
        
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
        self.assertTrue('pad_numer_porzadkowy' in sample_data[0])
        self.assertTrue(u'Gliwice' in sample_data[1])
        self.assertTrue(u'Ulica Dworcowa ' in sample_data[1])

    def test_single_jp_f(self):
        ''' Test complete process_one.process on Japanese sample data.
        '''
        source = join(self.src_dir, 'jp-fukushima.json')
        
        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir)
        
        with open(state_path) as file:
            state = dict(zip(*json.load(file)))
        
        self.assertTrue(state['sample'] is not None)
        
        with open(join(dirname(state_path), state['sample'])) as file:
            sample_data = json.load(file)
        
        self.assertEqual(len(sample_data), 6)
        self.assertTrue(u'大字・町丁目名' in sample_data[0])
        self.assertTrue(u'田沢字姥懐' in sample_data[1])
        self.assertTrue('37.706391' in sample_data[1])
        self.assertTrue('140.480007' in sample_data[1])

    def test_single_utah(self):
        ''' Test complete process_one.process on data that uses file selection with mixed case (issue #104)
        '''
        source = join(self.src_dir, 'us-ut.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertTrue(state['sample'] is not None)

        with open(join(dirname(state_path), state['sample'])) as file:
            sample_data = json.load(file)

        self.assertEqual(len(sample_data), 6)

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
        with open(filename, 'rb') as file:
            self.s3._write_fake_key(self.name, file.read())
