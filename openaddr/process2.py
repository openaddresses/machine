from urlparse import urlparse
from os.path import join, basename
from shutil import copy, move
from os import mkdir
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
    
    print result1.__dict__
    
    scheme, _, cache_path, _, _, _ = urlparse(result1.cache)
    
    conform_dir = join(temp_dir, 'conform')
    
    mkdir(conform_dir)
    move(cache_path, join(conform_dir, basename(cache_path)))
    
    print temp_src
    print join(conform_dir, basename(cache_path)), '!!!'
    
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
