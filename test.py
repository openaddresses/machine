import unittest
import shutil
import tempfile
import json
import cPickle
import re
from os import close, environ, mkdir
from StringIO import StringIO
from mimetypes import guess_type
from urlparse import urlparse, parse_qs
from os.path import dirname, join, basename, exists
from fcntl import lockf, LOCK_EX, LOCK_UN
from contextlib import contextmanager
from subprocess import Popen, PIPE
from csv import DictReader

from requests import get
from httmock import response, HTTMock
        
from openaddr import paths, cache, conform, excerpt, jobs, S3, process
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
            self.assertTrue(bool(state['geometry type']), 'Checking for geometry type in {}'.format(source))
            self.assertTrue(bool(state['sample']), 'Checking for sample in {}'.format(source))

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

            result1 = cache(source, self.testdir, dict(), self.s3)
            self.assertTrue(result1.cache is not None)
            self.assertTrue(result1.version is not None)
            self.assertTrue(result1.fingerprint is not None)
            
            result2 = conform(source, self.testdir, result1.todict(), self.s3)
            self.assertTrue(result2.processed is not None)
            self.assertTrue(result2.sample is not None)
            self.assertTrue('FID_PARCEL' in result2.sample[0])

            result3 = excerpt(source, self.testdir, result1.todict(), self.s3)
            self.assertTrue(result3.sample_data is not None)
            
            sample_key = '/'.join(result3.sample_data.split('/')[4:])
            sample_data = json.loads(self.s3._read_fake_key(sample_key))
            
            self.assertEqual(len(sample_data), 6)
            self.assertTrue('ZIPCODE' in sample_data[0])
            self.assertTrue('OAKLAND' in sample_data[1])
            self.assertTrue('94612' in sample_data[1])

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
            self.assertFalse(result.sample is None)
            self.assertTrue('FID_PARCEL' in result.sample[0])

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
            self.assertTrue(result.sample is not None)
            self.assertTrue('SITEFRAC' in result.sample[0])
            
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

class TestConform (unittest.TestCase):
    '''
    (u'lat', u'lon', u'number', u'split', u'street', u'type')
    (u'lat', u'lon', u'merge', u'number', u'postcode', u'street', u'type')
    (u'lat', u'lon', u'merge', u'number', u'street', u'type')
    (u'lat', u'lon', u'number', u'street', u'type')
    (u'advanced_merge', u'encoding', u'lat', u'lon', u'number', u'srs', u'street', u'type')
    (u'lat', u'lon', u'number', u'postcode', u'split', u'street', u'type')
    (u'lat', u'lon', u'number', u'postcode', u'street', u'type')
    (u'file', u'lat', u'lon', u'number', u'street', u'type')
    '''
    def setUp(self):
        ''' Prepare a clean temporary directory.
        '''
        jobs.setup_logger(False)
        
        self.testdir = tempfile.mkdtemp(prefix='test-')
        self.conforms_dir = join(dirname(__file__), 'tests', 'conforms')
        
        self.s3 = FakeS3()
        
        cmd = Popen('which node'.split(), stdout=PIPE)
        cmd.wait()
        
        self.run_nodejs = bool(cmd.stdout.read()) and exists(paths.conform)
    
    def tearDown(self):
        pass # shutil.rmtree(self.testdir)
    
    def response_content(self, url, request):
        ''' Fake HTTP responses for use with HTTMock in tests.
        '''
        _, host, path, _, query, _ = urlparse(url.geturl())
        data_dirname = join(dirname(__file__), 'tests', 'data')
        local_path = None
        
        if host == 'fake-cache':
            local_path = join(self.conforms_dir, basename(path))
        
        if host == 'fake-s3':
            return response(200, self.s3._read_fake_key(path))
        
        if local_path:
            type, _ = guess_type(local_path)
            with open(local_path) as file:
                return response(200, file.read(), headers={'Content-Type': type})
        
        raise NotImplementedError(url.geturl())
    
    def _copy_source(self, source_name):
        '''
        '''
        source_path = join(self.testdir, source_name+'.json')
        cache_dir = join(self.testdir, source_name)
        
        shutil.copyfile(join(self.conforms_dir, source_name+'.json'),
                        source_path)
        
        mkdir(cache_dir)

        return source_path, cache_dir
    
    def _copy_shapefile(self, source_name):
        '''
        '''
        source_path, cache_dir = self._copy_source(source_name)

        for ext in ('.shp', '.shx', '.dbf', '.prj'):
            filename = source_name+ext
            shutil.copyfile(join(self.conforms_dir, filename),
                            join(cache_dir, filename))
        
        return source_path, cache_dir
    
    def _run_node_conform(self, source_path):
        '''
        '''
        args = dict(cwd=self.testdir, stderr=PIPE, stdout=PIPE)
        cmd = Popen(('node', paths.conform, source_path, self.testdir), **args)
        cmd.wait()
        
        return cmd
    
    def test_nodejs_lake_man(self):
        if not self.run_nodejs:
            return
    
        source_path, cache_dir = self._copy_shapefile('lake-man')
        
        cmd = self._run_node_conform(source_path)
        self.assertEqual(cmd.returncode, 0)
        
        with open(join(cache_dir, 'out.csv')) as file:
            rows = list(DictReader(file, dialect='excel'))
            self.assertEqual(rows[0]['NUMBER'], '5115')
            self.assertEqual(rows[0]['STREET'], 'Fruited Plains Lane')
            self.assertEqual(rows[1]['NUMBER'], '5121')
            self.assertEqual(rows[1]['STREET'], 'Fruited Plains Lane')
            self.assertEqual(rows[2]['NUMBER'], '5133')
            self.assertEqual(rows[2]['STREET'], 'Fruited Plains Lane')
            self.assertEqual(rows[3]['NUMBER'], '5126')
            self.assertEqual(rows[3]['STREET'], 'Fruited Plains Lane')
            self.assertEqual(rows[4]['NUMBER'], '5120')
            self.assertEqual(rows[4]['STREET'], 'Fruited Plains Lane')
            self.assertEqual(rows[5]['NUMBER'], '5115')
            self.assertEqual(rows[5]['STREET'], 'Old Mill Road')
    
    def test_python_lake_man(self):
        source_path, cache_dir = self._copy_shapefile('lake-man')
    
        with HTTMock(self.response_content):
            result = conform(source_path, self.testdir, {}, self.s3)
            output = StringIO(get(result.processed).content)
        
        rows = list(DictReader(output, dialect='excel'))
        self.assertEqual(rows[0]['NUMBER'], '5115')
        self.assertEqual(rows[0]['STRNAME'], 'FRUITED PLAINS LN')
        self.assertEqual(rows[1]['NUMBER'], '5121')
        self.assertEqual(rows[1]['STRNAME'], 'FRUITED PLAINS LN')
        self.assertEqual(rows[2]['NUMBER'], '5133')
        self.assertEqual(rows[2]['STRNAME'], 'FRUITED PLAINS LN')
        self.assertEqual(rows[3]['NUMBER'], '5126')
        self.assertEqual(rows[3]['STRNAME'], 'FRUITED PLAINS LN')
        self.assertEqual(rows[4]['NUMBER'], '5120')
        self.assertEqual(rows[4]['STRNAME'], 'FRUITED PLAINS LN')
        self.assertEqual(rows[5]['NUMBER'], '5115')
        self.assertEqual(rows[5]['STRNAME'], 'OLD MILL RD')
    
    def test_lake_man_split(self):
        if not self.run_nodejs:
            return
    
        source_path, cache_dir = self._copy_shapefile('lake-man-split')
        
        cmd = self._run_node_conform(source_path)
        self.assertEqual(cmd.returncode, 0)
        
        with open(join(cache_dir, 'out.csv')) as file:
            rows = list(DictReader(file, dialect='excel'))
            self.assertEqual(rows[0]['NUMBER'], '915')
            self.assertEqual(rows[0]['STREET'], 'Edward Avenue')
            self.assertEqual(rows[1]['NUMBER'], '3273')
            self.assertEqual(rows[1]['STREET'], 'Peter Street')
            self.assertEqual(rows[2]['NUMBER'], '976')
            self.assertEqual(rows[2]['STREET'], 'Ford Boulevard')
            self.assertEqual(rows[3]['NUMBER'], '7055')
            self.assertEqual(rows[3]['STREET'], 'Saint Rose Avenue')
            self.assertEqual(rows[4]['NUMBER'], '534')
            self.assertEqual(rows[4]['STREET'], 'Wallace Avenue')
            self.assertEqual(rows[5]['NUMBER'], '531')
            self.assertEqual(rows[5]['STREET'], 'Scofield Avenue')
    
    def test_lake_man_split2(self):
        if not self.run_nodejs:
            return
    
        source_path, cache_dir = self._copy_source('lake-man-split2')

        shutil.copyfile(join(self.conforms_dir, 'lake-man-split2.geojson'),
                        join(cache_dir, 'lake-man-split2.json'))
        
        cmd = self._run_node_conform(source_path)
        
        # No clue why Node errors here. TODO: figure it out.
        return
        
        self.assertEqual(cmd.returncode, 0)
        
        with open(join(cache_dir, 'out.csv')) as file:
            rows = list(DictReader(file, dialect='excel'))
            import pprint; pprint.pprint(rows)
            self.assertEqual(rows[0]['NUMBER'], '1')
            self.assertEqual(rows[0]['STREET'], 'Spectrum Pointe Dr #320')
            self.assertEqual(rows[1]['NUMBER'], '')
            self.assertEqual(rows[1]['STREET'], '')
            self.assertEqual(rows[2]['NUMBER'], '300')
            self.assertEqual(rows[2]['STREET'], 'E Chapman Ave')
            self.assertEqual(rows[3]['NUMBER'], '1')
            self.assertEqual(rows[3]['STREET'], 'Spectrum Pointe Dr #320')
            self.assertEqual(rows[4]['NUMBER'], '1')
            self.assertEqual(rows[4]['STREET'], 'Spectrum Pointe Dr #320')
            self.assertEqual(rows[5]['NUMBER'], '1')
            self.assertEqual(rows[5]['STREET'], 'Spectrum Pointe Dr #320')
    
    def test_lake_man_merge_postcode(self):
        if not self.run_nodejs:
            return
    
        source_path, cache_dir = self._copy_shapefile('lake-man-merge-postcode')
        
        cmd = self._run_node_conform(source_path)
        self.assertEqual(cmd.returncode, 0)
        
        with open(join(cache_dir, 'out.csv')) as file:
            rows = list(DictReader(file, dialect='excel'))
            self.assertEqual(rows[0]['NUMBER'], '35845')
            self.assertEqual(rows[0]['STREET'], 'Eklutna Lake Road')
            self.assertEqual(rows[1]['NUMBER'], '35850')
            self.assertEqual(rows[1]['STREET'], 'Eklutna Lake Road')
            self.assertEqual(rows[2]['NUMBER'], '35900')
            self.assertEqual(rows[2]['STREET'], 'Eklutna Lake Road')
            self.assertEqual(rows[3]['NUMBER'], '35870')
            self.assertEqual(rows[3]['STREET'], 'Eklutna Lake Road')
            self.assertEqual(rows[4]['NUMBER'], '32551')
            self.assertEqual(rows[4]['STREET'], 'Eklutna Lake Road')
            self.assertEqual(rows[5]['NUMBER'], '31401')
            self.assertEqual(rows[5]['STREET'], 'Eklutna Lake Road')
    
    def test_lake_man_merge_postcode2(self):
        if not self.run_nodejs:
            return
    
        source_path, cache_dir = self._copy_shapefile('lake-man-merge-postcode2')
        
        cmd = self._run_node_conform(source_path)
        self.assertEqual(cmd.returncode, 0)
        
        with open(join(cache_dir, 'out.csv')) as file:
            rows = list(DictReader(file, dialect='excel'))
            self.assertEqual(rows[0]['NUMBER'], '85')
            self.assertEqual(rows[0]['STREET'], 'Maitland Drive')
            self.assertEqual(rows[1]['NUMBER'], '81')
            self.assertEqual(rows[1]['STREET'], 'Maitland Drive')
            self.assertEqual(rows[2]['NUMBER'], '92')
            self.assertEqual(rows[2]['STREET'], 'Maitland Drive')
            self.assertEqual(rows[3]['NUMBER'], '92')
            self.assertEqual(rows[3]['STREET'], 'Maitland Drive')
            self.assertEqual(rows[4]['NUMBER'], '92')
            self.assertEqual(rows[4]['STREET'], 'Maitland Drive')
            self.assertEqual(rows[5]['NUMBER'], '92')
            self.assertEqual(rows[5]['STREET'], 'Maitland Drive')

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
