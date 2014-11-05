import unittest
import shutil
import tempfile
import json
import pickle
from uuid import uuid4
from os import environ, close
from StringIO import StringIO
from urlparse import urlparse
from os.path import dirname, join, splitext
from csv import DictReader
from glob import glob

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

        # Rename sources with random characters so S3 keys are unique.
        self.uuid = uuid4().hex

        for path in glob(join(self.src_dir, '*.json')):
            base, ext = splitext(path)
            shutil.move(path, '{0}-{1}{2}'.format(base, self.uuid, ext))
        
        self.s3 = FakeS3(environ['AWS_ACCESS_KEY_ID'], environ['AWS_SECRET_ACCESS_KEY'], 'data-test.openaddresses.io')
    
    def tearDown(self):
        shutil.rmtree(self.testdir)

    def response_content(self, url, request):
        
        _, host, path, _, _, _ = urlparse(url.geturl())
        
        if path.endswith('sources/us-ca-oakland-excerpt.zip'):
            local_path = join('tests', 'data', 'us-ca-oakland-excerpt.zip')
            with open(join(dirname(__file__), local_path)) as file:
                return response(200, file.read())
        
        elif path.endswith('sources/us-ca-alameda_county-excerpt.zip'):
            local_path = join('tests', 'data', 'us-ca-alameda_county-excerpt.zip')
            with open(join(dirname(__file__), local_path)) as file:
                return response(200, file.read())
        
        elif path.endswith('-excerpt.zip'):
            return response(200, self.s3._read_fake_key(path))
        
        raise NotImplementedError(host, path)
    
    def test_parallel(self):
        process.process(self.s3, self.src_dir, 'test')
        
        # Go looking for state.txt in fake S3.
        buffer = StringIO(self.s3.keys['runs/test/state.txt'])
        states = dict([(row['source'], row) for row
                       in DictReader(buffer, dialect='excel-tab')])
        
        for (source, state) in states.items():
            self.assertTrue(bool(state['cache']))
            self.assertTrue(bool(state['version']))
            self.assertTrue(bool(state['fingerprint']))

            if 'san_francisco' in source or 'alameda_county' in source:
                self.assertTrue(bool(state['processed']), "state['processed'] should not be empty in {}".format(source))
            else:
                self.assertFalse(bool(state['processed']), "state['processed'] should be empty in {}".format(source))
    
    def test_single_ac(self):
        with HTTMock(self.response_content):
            source = join(self.src_dir, 'us-ca-alameda_county-{0}.json'.format(self.uuid))

            result = cache(source, self.testdir, dict(), self.s3)
            self.assertTrue(result.cache is not None)
            self.assertTrue(result.version is not None)
            self.assertTrue(result.fingerprint is not None)
        
            result = conform(source, self.testdir, result.todict(), self.s3)
            self.assertTrue(result.processed is not None)

    def test_single_oak(self):
        with HTTMock(self.response_content):
            source = join(self.src_dir, 'us-ca-oakland-{0}.json'.format(self.uuid))

            result = cache(source, self.testdir, dict(), self.s3)
            self.assertTrue(result.cache is not None)
            self.assertTrue(result.version is not None)
            self.assertTrue(result.fingerprint is not None)
        
            result = conform(source, self.testdir, result.todict(), self.s3)
            self.assertTrue(result.processed is None)

class FakeS3 (S3):
    ''' Just enough S3 to work for tests.
    '''
    _fake_keys = None
    
    def __init__(self, *args, **kwargs):
        handle, self._fake_keys = tempfile.mkstemp(prefix='fakeS3-', suffix='.pickle')
        close(handle)
        
        with open(self._fake_keys, 'w') as file:
            pickle.dump(dict(), file)

        S3.__init__(self, *args, **kwargs)
    
    def _write_fake_key(self, name, string):
        with open(self._fake_keys, 'r') as file:
            data = pickle.load(file)
            
        data[name] = string
        
        with open(self._fake_keys, 'w') as file:
            pickle.dump(data, file)
    
    def _read_fake_key(self, name):
        with open(self._fake_keys, 'r') as file:
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
        return 'http://example.local' + self.name

    def set_contents_from_string(self, string, **kwargs):
        self.s3._write_fake_key(self.name, string)
        
    def set_contents_from_filename(self, filename, **kwargs):
        with open(filename) as file:
            self.s3._write_fake_key(self.name, file.read())

if __name__ == '__main__':
    unittest.main()
