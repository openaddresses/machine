import unittest
import shutil
import tempfile
import json
from uuid import uuid4
from os import environ
from os.path import dirname, join, splitext
from glob import glob

from openaddr import cache, conform, jobs, S3

class TestCache (unittest.TestCase):
    
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
        
        self.s3 = S3(environ['AWS_ACCESS_KEY_ID'], environ['AWS_SECRET_ACCESS_KEY'], 'openaddresses-tests')
    
    def tearDown(self):
        shutil.rmtree(self.testdir)
    
    def test_parallel(self):
        sources1 = glob(join(self.src_dir, '*.json'))
        source_extras1 = dict([(s, dict()) for s in sources1])
        results1 = jobs.run_all_caches(sources1, source_extras1, self.s3)
        
        # Proceed only with sources that have a cache
        sources2 = [s for s in sources1 if results1[s].cache]
        source_extras2 = dict([(s, results1[s].todict()) for s in sources2])
        results2 = jobs.run_all_conforms(sources2, source_extras2, self.s3)
    
        for (source, result) in results1.items():
            # OpenAddresses-Cache will add three keys to the source file.
            self.assertTrue(hasattr(result, 'cache'))
            self.assertTrue(hasattr(result, 'version'))
            self.assertTrue(hasattr(result, 'fingerprint'))
    
        for (source, result) in results2.items():
            # OpenAddresses-Conform will add a processed key to the
            # source file, if the conform data was present initially.
            self.assertTrue(hasattr(result, 'processed'))
            self.assertTrue(hasattr(result, 'path'))
            if 'san_francisco' in source or 'alameda_county' in source:
                self.assertTrue(type(result.processed) in (str, unicode))
            else:
                self.assertTrue(result.processed is None)

    def test_single_ac(self):
        source = join(self.src_dir, 'us-ca-alameda_county-{0}.json'.format(self.uuid))

        result = cache(source, self.testdir, dict(), self.s3)
        self.assertTrue(result.cache is not None)
        self.assertTrue(result.version is not None)
        self.assertTrue(result.fingerprint is not None)
        
        result = conform(source, self.testdir, result.todict(), self.s3)
        self.assertTrue(result.processed is not None)
        self.assertTrue(result.path is not None)

    def test_single_oak(self):
        source = join(self.src_dir, 'us-ca-oakland-{0}.json'.format(self.uuid))

        result = cache(source, self.testdir, dict(), self.s3)
        self.assertTrue(result.cache is not None)
        self.assertTrue(result.version is not None)
        self.assertTrue(result.fingerprint is not None)
        
        result = conform(source, self.testdir, result.todict(), self.s3)
        self.assertTrue(result.processed is None)
        self.assertTrue(result.path is None)

if __name__ == '__main__':
    unittest.main()
