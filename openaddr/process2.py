from urlparse import urlparse
from os.path import join, basename, dirname, exists
from shutil import copy, move
from logging import getLogger
from os import mkdir, rmdir
import tempfile

from . import cache, conform, excerpt
from .jobs import setup_logger

def process(source, destination):
    '''
    '''
    temp_dir = tempfile.mkdtemp(prefix='process2-')
    temp_src = join(temp_dir, basename(source))
    copy(source, temp_src)
    
    result1 = cache(temp_src, temp_dir, dict(), False)
    
    scheme, _, cache_path1, _, _, _ = urlparse(result1.cache)
    if scheme != 'file':
        raise RuntimeError('Nothing cached? {}'.format(result1.cache))
    
    getLogger('openaddr').info('Cached data in {}'.format(result1.cache))

    #
    #
    #
    result2 = conform(temp_src, temp_dir, result1.todict(), False)
    
    if not exists(result2.path):
        raise RuntimeError('Nothing processed? {}'.format(result2.path))
    
    getLogger('openaddr').info('Processed data in {}'.format(result2.path))
    
    return 1
    
    self.assertTrue(result1.cache is not None)
    self.assertTrue(result1.version is not None)
    self.assertTrue(result1.fingerprint is not None)
    
    result2 = conform(source, temp_dir, result1.todict(), self.s3)
    self.assertTrue(result2.processed is not None)
    self.assertTrue(result2.path is not None)

    result3 = excerpt(source, temp_dir, result1.todict(), self.s3)
    self.assertTrue(result3.sample_data is not None)
    self.assertEqual(result3.geometry_type, 'Point')

def main():
    '''
    '''
    source = 'tests/sources/us-ca-alameda_county.json'
    destination = 'out'
    
    setup_logger(None)
    
    return process(source, destination)

if __name__ == '__main__':
    exit(main())
