import unittest
import shutil
import tempfile
import json
from uuid import uuid4
from os import environ
from StringIO import StringIO
from os.path import dirname, join, splitext
from csv import DictReader
from glob import glob

from openaddr import cache, conform, excerpt, jobs, S3, process
from openaddr.sample import TestSample

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
            self.assertTrue(bool(state['sample']))

            if 'san_francisco' in source or 'alameda_county' in source or 'polk' in source:
                self.assertTrue(bool(state['processed']))
            else:
                self.assertFalse(bool(state['processed']))
    
    def test_single_ac(self):
        source = join(self.src_dir, 'us-ca-alameda_county-{0}.json'.format(self.uuid))

        result1 = cache(source, self.testdir, dict(), self.s3)
        self.assertTrue(result1.cache is not None)
        self.assertTrue(result1.version is not None)
        self.assertTrue(result1.fingerprint is not None)
        
        result2 = conform(source, self.testdir, result1.todict(), self.s3)
        self.assertTrue(result2.processed is not None)
        self.assertTrue(result2.path is not None)

        result3 = excerpt(source, self.testdir, result1.todict(), self.s3)
        self.assertTrue(result3.sample_data is not None)
        
        sample_key = '/'.join(result3.sample_data.split('/')[4:])
        sample_data = json.loads(self.s3.keys[sample_key])
        
        self.assertEqual(len(sample_data), 6)
        self.assertTrue('ZIPCODE' in sample_data[0])
        self.assertTrue('OAKLAND' in sample_data[1])
        self.assertTrue('94612' in sample_data[1])

    def test_single_polk(self):
        source = join(self.src_dir, 'us-ia-polk-{0}.json'.format(self.uuid))

        result1 = cache(source, self.testdir, dict(), self.s3)
        self.assertTrue(result1.cache is not None)
        self.assertTrue(result1.version is not None)
        self.assertTrue(result1.fingerprint is not None)
        
        result2 = conform(source, self.testdir, result1.todict(), self.s3)
        self.assertTrue(result2.processed is not None)
        self.assertTrue(result2.path is not None)

        result3 = excerpt(source, self.testdir, result1.todict(), self.s3)
        self.assertTrue(result3.sample_data is not None)
        
        sample_key = '/'.join(result3.sample_data.split('/')[4:])
        sample_data = json.loads(self.s3.keys[sample_key])
        
        self.assertEqual(len(sample_data), 6)
        self.assertTrue('zip' in sample_data[0])
        self.assertTrue('IA' in sample_data[1])

    def test_single_oak(self):
        source = join(self.src_dir, 'us-ca-oakland-{0}.json'.format(self.uuid))

        result = cache(source, self.testdir, dict(), self.s3)
        self.assertTrue(result.cache is not None)
        self.assertTrue(result.version is not None)
        self.assertTrue(result.fingerprint is not None)
        
        result = conform(source, self.testdir, result.todict(), self.s3)
        self.assertTrue(result.processed is None)
        self.assertTrue(result.path is None)

class FakeS3 (S3):
    ''' Just enough S3 to work for tests.
    '''
    keys = None
    
    def __init__(self, *args, **kwargs):
        self.keys = dict()
        S3.__init__(self, *args, **kwargs)
    
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
    def __init__(self, name, fake_s3):
        self.name = name
        self.s3 = fake_s3

    def set_contents_from_string(self, string, **kwargs):
        self.s3.keys[self.name] = string

if __name__ == '__main__':
    unittest.main()
