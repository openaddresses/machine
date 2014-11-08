import unittest
import shutil
import tempfile
import json
import cPickle
import re
from os import close
from StringIO import StringIO
from mimetypes import guess_type
from urlparse import urlparse, parse_qs
from os.path import dirname, join
from fcntl import lockf, LOCK_EX, LOCK_UN
from contextlib import contextmanager
from csv import DictReader

from httmock import response, HTTMock

from openaddr import cache, conform, jobs, S3, process

class TestOA (unittest.TestCase):
    
    def setUp(self):
        ''' Prepare a clean temporary directory, and copy sources there.
        '''
        jobs.setup_logger(False)

        self.testdir = tempfile.mkdtemp(prefix='test-')
        self.src_dir = join(self.testdir, 'sources')
        sources_dir = join(dirname(__file__), 'tests', 'sources')
        shutil.copytree(sources_dir, self.src_dir)

        self.s3 = FakeS3()
    
    def tearDown(self):
        shutil.rmtree(self.testdir)

    def response_content(self, url, request):
        ''' Fake HTTP responses for use with HTTMock in tests.
        '''
        _, host, path, _, query, _ = urlparse(url.geturl())
        data_dirname = join(dirname(__file__), 'tests', 'data')
        local_path = None
        
        if host == 'fake-s3':
            return response(200, self.s3._read_fake_key(path))
        
        if (host, path) == ('data.acgov.org', '/api/geospatial/8e4s-7f4v'):
            local_path = join(data_dirname, 'us-ca-alameda_county-excerpt.zip')
        
        if (host, path) == ('www.ci.berkeley.ca.us', '/uploadedFiles/IT/GIS/Parcels.zip'):
            local_path = join(data_dirname, 'us-ca-berkeley-excerpt.zip')
        
        if (host, path) == ('data.openoakland.org', '/sites/default/files/OakParcelsGeo2013_0.zip'):
            local_path = join(data_dirname, 'us-ca-oakland-excerpt.zip')
        
        if (host, path) == ('data.sfgov.org', '/download/kvej-w5kb/ZIPPED%20SHAPEFILE'):
            local_path = join(data_dirname, 'us-ca-san_francisco-excerpt.zip')
        
        if (host, path) == ('www.carsonproperty.info', '/ArcGIS/rest/services/basemap/MapServer/1/query'):
            where_clause = parse_qs(query)['where'][0]
            if where_clause == 'objectid >= 0 and objectid < 500':
                local_path = join(data_dirname, 'us-ca-carson-0.json')
            elif where_clause == 'objectid >= 500 and objectid < 1000':
                local_path = join(data_dirname, 'us-ca-carson-1.json')
        
        if local_path:
            type, _ = guess_type(local_path)
            with open(local_path) as file:
                return response(200, file.read(), headers={'Content-Type': type})
        
        raise NotImplementedError(url.geturl())
    
    def test_process(self):
        ''' Test process.process(), with complete threaded behavior.
        '''
        with HTTMock(self.response_content):
            process.process(self.s3, self.src_dir, 'test')
        
        # Go looking for state.txt in fake S3.
        buffer = StringIO(self.s3._read_fake_key('runs/test/state.txt'))
        states = dict([(row['source'], row) for row
                       in DictReader(buffer, dialect='excel-tab')])
        
        print self.s3._read_fake_key('runs/test/state.txt')
        
        for (source, state) in states.items():
            self.assertTrue(bool(state['cache']), 'Checking for cache in {}'.format(source))
            self.assertTrue(bool(state['version']), 'Checking for version in {}'.format(source))
            self.assertTrue(bool(state['fingerprint']), 'Checking for fingerprint in {}'.format(source))

            if 'san_francisco' in source or 'alameda_county' in source:
                self.assertTrue(bool(state['processed']), "Checking for processed in {}".format(source))
            else:
                # This might actually need to be false?
                self.assertTrue(bool(state['processed']), "Checking for processed in {}".format(source))
    
    def test_single_ac(self):
        ''' Test cache() and conform() on Alameda County sample data.
        '''
        with HTTMock(self.response_content):
            source = join(self.src_dir, 'us-ca-alameda_county.json')

            result = cache(source, self.testdir, dict(), self.s3)
            self.assertTrue(result.cache is not None)
            self.assertTrue(result.version is not None)
            self.assertTrue(result.fingerprint is not None)
        
            result = conform(source, self.testdir, result.todict(), self.s3)
            self.assertTrue(result.processed is not None)
            
            _, _, path, _, _, _ = urlparse(result.processed)
            self.assertTrue('2000 BROADWAY' in self.s3._read_fake_key(path))

    def test_single_oak(self):
        ''' Test cache() and conform() on Oakland sample data.
        '''
        with HTTMock(self.response_content):
            source = join(self.src_dir, 'us-ca-oakland.json')

            result = cache(source, self.testdir, dict(), self.s3)
            self.assertTrue(result.cache is not None)
            self.assertTrue(result.version is not None)
            self.assertTrue(result.fingerprint is not None)
        
            result = conform(source, self.testdir, result.todict(), self.s3)
            
            # the content of result.processed does not currently have addresses.
            self.assertFalse(result.processed is None)

    def test_single_car(self):
        ''' Test cache() and conform() on Carson sample data.
        '''
        with HTTMock(self.response_content):
            source = join(self.src_dir, 'us-ca-carson.json')

            result = cache(source, self.testdir, dict(), self.s3)
            self.assertTrue(result.cache is not None)
            self.assertTrue(result.version is not None)
            self.assertTrue(result.fingerprint is not None)
        
            result = conform(source, self.testdir, result.todict(), self.s3)
            self.assertTrue(result.processed is not None)
            
            _, _, path, _, _, _ = urlparse(result.processed)
            self.assertTrue('555 E CARSON ST' in self.s3._read_fake_key(path))

@contextmanager
def locked_open(filename):
    ''' Open and lock a file, for use with threads and processes.
    '''
    with open(filename, 'r+') as file:
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
        
        with open(self._fake_keys, 'w') as file:
            cPickle.dump(dict(), file)

        S3.__init__(self, 'Fake Key', 'Fake Secret', 'data-test.openaddresses.io')
    
    def _write_fake_key(self, name, string):
        with locked_open(self._fake_keys) as file:
            data = cPickle.load(file)
            data[name] = string
            
            file.seek(0)
            file.truncate()
            cPickle.dump(data, file)
    
    def _read_fake_key(self, name):
        with locked_open(self._fake_keys) as file:
            data = cPickle.load(file)
            
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
        return 'http://fake-s3' + self.name

    def set_contents_from_string(self, string, **kwargs):
        self.s3._write_fake_key(self.name, string)
        
    def set_contents_from_filename(self, filename, **kwargs):
        with open(filename) as file:
            self.s3._write_fake_key(self.name, file.read())

if __name__ == '__main__':
    unittest.main()
