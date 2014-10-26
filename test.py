import unittest
import shutil
import tempfile
import json
from uuid import uuid4
from os.path import dirname, join, splitext
from glob import glob

from openaddr import cache, conform, jobs

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
    
    def tearDown(self):
        shutil.rmtree(self.testdir)
    
    def test_parallel(self):
        sources1 = glob(join(self.src_dir, '*.json'))
        caches = jobs.run_all_caches(sources1, 'openaddresses-tests')
        
        # Proceed only with sources that have a cache
        sources2 = [s for s in sources1 if caches[s]['cache']]
        source_extras = dict([(s, caches[s]) for s in sources2])
        conformed = jobs.run_all_conforms(sources2, source_extras, 'openaddresses-tests')
    
        for (source, cache) in caches.items():
            # OpenAddresses-Cache will add three keys to the source file.
            self.assertTrue('cache' in cache)
            self.assertTrue('version' in cache)
            self.assertTrue('fingerprint' in cache)
    
        for (source, conform) in conformed.items():
            # OpenAddresses-Conform will add a processed key to the
            # source file, if the conform data was present initially.
            self.assertTrue('processed' in conform)
            self.assertTrue('path' in conform)
            if 'san_francisco' in source or 'alameda_county' in source:
                self.assertTrue(type(conform['processed']) in (str, unicode))
            else:
                self.assertTrue(conform['processed'] is None)

    def test_single_ac(self):
        source = join(self.src_dir, 'us-ca-alameda_county-{0}.json'.format(self.uuid))

        result = cache(source, self.testdir, 'openaddresses-tests')
        self.assertTrue(result['cache'] is not None)
        self.assertTrue(result['version'] is not None)
        self.assertTrue(result['fingerprint'] is not None)
        
        result = conform(source, self.testdir, result, 'openaddresses-tests')
        self.assertTrue(result['processed'] is not None)
        self.assertTrue(result['path'] is not None)

    def test_single_oak(self):
        source = join(self.src_dir, 'us-ca-oakland-{0}.json'.format(self.uuid))

        result = cache(source, self.testdir, 'openaddresses-tests')
        self.assertTrue(result['cache'] is not None)
        self.assertTrue(result['version'] is not None)
        self.assertTrue(result['fingerprint'] is not None)
        
        result = conform(source, self.testdir, result, 'openaddresses-tests')
        self.assertTrue(result['processed'] is None)
        self.assertTrue(result['path'] is None)

if __name__ == '__main__':
    unittest.main()
