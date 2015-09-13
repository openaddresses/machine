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
from ..compat import standard_library, csvopen, csvDictReader

import unittest
import shutil
import tempfile
import json
import re
import pickle
import sys
from os import close, environ, mkdir, remove
from io import BytesIO
from csv import DictReader
from zipfile import ZipFile
from mimetypes import guess_type
from urllib.parse import urlparse, parse_qs
from os.path import dirname, join, basename, exists, splitext
from contextlib import contextmanager
from subprocess import Popen, PIPE
from threading import Lock

if sys.platform != 'win32':
    from fcntl import lockf, LOCK_EX, LOCK_UN
else:
    lockf, LOCK_EX, LOCK_UN = None, None, None

from requests import get
from httmock import response, HTTMock
import mock
        
from .. import cache, conform, S3, process_one, package_output

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
        
        if (host, path) == ('data.openaddresses.io', '/cache/fr/BAN_licence_gratuite_repartage_75.zip'):
            local_path = join(data_dirname, 'BAN_licence_gratuite_repartage_75.zip')
        
        if (host, path) == ('data.openaddresses.io', '/cache/fr/BAN_licence_gratuite_repartage_974.zip'):
            local_path = join(data_dirname, 'BAN_licence_gratuite_repartage_974.zip')
        
        if scheme == 'file':
            local_path = path

        if local_path:
            type, _ = guess_type(local_path)
            with open(local_path, 'rb') as file:
                return response(200, file.read(), headers={'Content-Type': type})
        
        raise NotImplementedError(url.geturl())
    
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
        self.assertIsNone(state['website'])
        self.assertEqual(state['license'], 'http://www.acgov.org/acdata/terms.htm')
        
        with open(join(dirname(state_path), state['sample'])) as file:
            sample_data = json.load(file)
        
        self.assertEqual(len(sample_data), 6)
        self.assertTrue('ZIPCODE' in sample_data[0])
        self.assertTrue('OAKLAND' in sample_data[1])
        self.assertTrue('94612' in sample_data[1])
        
        output_path = join(dirname(state_path), state['processed'])
        
        with csvopen(output_path, encoding='utf8') as input:
            rows = list(csvDictReader(input, encoding='utf8'))
            self.assertEqual(rows[1]['ID'], '')
            self.assertEqual(rows[10]['ID'], '')
            self.assertEqual(rows[100]['ID'], '')
            self.assertEqual(rows[1000]['ID'], '')
            self.assertEqual(rows[1]['NUMBER'], '2147')
            self.assertEqual(rows[10]['NUMBER'], '605')
            self.assertEqual(rows[100]['NUMBER'], '167')
            self.assertEqual(rows[1000]['NUMBER'], '322')
            self.assertEqual(rows[1]['STREET'], 'Broadway')
            self.assertEqual(rows[10]['STREET'], 'Hillsborough Street')
            self.assertEqual(rows[100]['STREET'], '8th Street')
            self.assertEqual(rows[1000]['STREET'], 'Hanover Avenue')

    def test_single_sf(self):
        ''' Test complete process_one.process on San Francisco sample data.
        '''
        source = join(self.src_dir, 'us-ca-san_francisco.json')
        
        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir)
        
        with open(state_path) as file:
            state = dict(zip(*json.load(file)))
        
        self.assertTrue(state['cache'] is not None)
        self.assertTrue(state['processed'] is not None)
        self.assertTrue(state['sample'] is not None)
        self.assertEqual(state['geometry type'], 'Point')
        self.assertIsNone(state['website'])
        self.assertEqual(state['license'], '')
        
        with open(join(dirname(state_path), state['sample'])) as file:
            sample_data = json.load(file)
        
        self.assertEqual(len(sample_data), 6)
        self.assertTrue('ZIPCODE' in sample_data[0])
        self.assertTrue('94102' in sample_data[1])
        
        output_path = join(dirname(state_path), state['processed'])
        
        with csvopen(output_path, encoding='utf8') as input:
            rows = list(csvDictReader(input, encoding='utf8'))
            self.assertEqual(rows[1]['ID'], '')
            self.assertEqual(rows[10]['ID'], '')
            self.assertEqual(rows[100]['ID'], '')
            self.assertEqual(rows[1000]['ID'], '')
            self.assertEqual(rows[1]['NUMBER'], '27')
            self.assertEqual(rows[10]['NUMBER'], '42')
            self.assertEqual(rows[100]['NUMBER'], '209')
            self.assertEqual(rows[1000]['NUMBER'], '1415')
            self.assertEqual(rows[1]['STREET'], 'Octavia Street')
            self.assertEqual(rows[10]['STREET'], 'Golden Gate Avenue')
            self.assertEqual(rows[100]['STREET'], 'Octavia Street')
            self.assertEqual(rows[1000]['STREET'], 'Folsom Street')

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
        self.assertEqual(state['website'], 'http://ci.carson.ca.us/')
        self.assertIsNone(state['license'])
        
        with open(join(dirname(state_path), state['sample'])) as file:
            sample_data = json.load(file)
        
        self.assertEqual(len(sample_data), 6)
        self.assertTrue('SITEFRAC' in sample_data[0])
        
        with open(join(dirname(state_path), state['processed'])) as file:
            rows = list(DictReader(file, dialect='excel'))
            self.assertEqual(5, len(rows))
            self.assertEqual(rows[0]['NUMBER'], '555')
            self.assertEqual(rows[0]['STREET'], 'Carson Street')
            self.assertEqual(rows[0]['CITY'], 'CARSON, CA')
            self.assertEqual(rows[0]['POSTCODE'], '90745')
            self.assertEqual(rows[0]['DISTRICT'], '')
            self.assertEqual(rows[0]['REGION'], '')
            self.assertEqual(rows[0]['ID'], '')

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
        
        self.assertFalse(state['skipped'])
        self.assertIsNotNone(state['cache'])
        # This test data does not contain a working conform object
        self.assertIsNone(state['processed'])
        self.assertEqual(state['website'], 'http://data.openoakland.org/dataset/property-parcels/resource/df20b818-0d16-4da8-a9c1-a7b8b720ff49')
        self.assertIsNone(state['license'])
        
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
        
        # This test data says "skip": True
        self.assertTrue(state['skipped'])
        self.assertIsNone(state['cache'])
        self.assertIsNone(state['processed'])

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
        self.assertEqual(state['website'], 'http://www.ci.berkeley.ca.us/datacatalog/')
        self.assertIsNone(state['license'])
        
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
        
    def test_single_berk_apn(self):
        ''' Test complete process_one.process on Berkeley sample data.
        '''
        source = join(self.src_dir, 'us-ca-berkeley-apn.json')
        
        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir)
        
        with open(state_path) as file:
            state = dict(zip(*json.load(file)))
        
        self.assertIsNotNone(state['cache'])
        self.assertIsNotNone(state['processed'])
        self.assertEqual(state['website'], 'http://www.ci.berkeley.ca.us/datacatalog/')
        self.assertIsNone(state['license'])
        
        output_path = join(dirname(state_path), state['processed'])
        
        with csvopen(output_path, encoding='utf8') as input:
            rows = list(csvDictReader(input, encoding='utf8'))
            self.assertEqual(rows[1]['ID'], '055 188300600')
            self.assertEqual(rows[10]['ID'], '055 189504000')
            self.assertEqual(rows[100]['ID'], '055 188700100')
            self.assertEqual(rows[1]['NUMBER'], '2418')
            self.assertEqual(rows[10]['NUMBER'], '2029')
            self.assertEqual(rows[100]['NUMBER'], '2298')
            self.assertEqual(rows[1]['STREET'], 'Dana Street')
            self.assertEqual(rows[10]['STREET'], 'Channing Way')
            self.assertEqual(rows[100]['STREET'], 'Durant Avenue')

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
        self.assertIsNone(state['website'])
        self.assertEqual(state['license'][:21], 'Polish Law on Geodesy')
        
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
        self.assertIsNone(state['website'])
        self.assertEqual(state['license'][:21], 'Polish Law on Geodesy')
        
        with open(join(dirname(state_path), state['sample'])) as file:
            sample_data = json.load(file)
        
        self.assertEqual(len(sample_data), 6)
        self.assertTrue('pad_numer_porzadkowy' in sample_data[0])
        self.assertTrue(u'Gliwice' in sample_data[1])
        self.assertTrue(u'Ulica Dworcowa ' in sample_data[1])
        
        output_path = join(dirname(state_path), state['processed'])
        
        with csvopen(output_path, encoding='utf8') as input:
            rows = list(csvDictReader(input, encoding='utf8'))
            self.assertEqual(rows[1]['NUMBER'], u'5')
            self.assertEqual(rows[10]['NUMBER'], u'8')
            self.assertEqual(rows[100]['NUMBER'], u'5a')
            self.assertEqual(rows[1]['STREET'], u'Ulica Dolnych Wa\u0142\xf3w  Gliwice')
            self.assertEqual(rows[10]['STREET'], u'Ulica Dolnych Wa\u0142\xf3w  Gliwice')
            self.assertEqual(rows[100]['STREET'], u'Plac Place Inwalid\xf3w Wojennych  Gliwice')

    def test_single_jp_f(self):
        ''' Test complete process_one.process on Japanese sample data.
        '''
        source = join(self.src_dir, 'jp-fukushima.json')
        
        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir)
        
        with open(state_path) as file:
            state = dict(zip(*json.load(file)))
        
        self.assertTrue(state['sample'] is not None)
        self.assertEqual(state['website'], 'http://nlftp.mlit.go.jp/isj/index.html')
        self.assertEqual(state['license'], 'http://nlftp.mlit.go.jp/ksj/other/yakkan.html')
        
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
        self.assertIsNone(state['website'])
        self.assertIsNone(state['license'])

        with open(join(dirname(state_path), state['sample'])) as file:
            sample_data = json.load(file)

        self.assertEqual(len(sample_data), 6)

    def test_single_fr_paris(self):
        ''' Test complete process_one.process on data that uses conform csvsplit (issue #124)
        '''
        source = join(self.src_dir, 'fr-paris.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertTrue(state['sample'] is not None)
        self.assertEqual(state['website'], 'http://adresse.data.gouv.fr/download/')
        self.assertIsNone(state['license'])

        with open(join(dirname(state_path), state['sample'])) as file:
            sample_data = json.load(file)

        self.assertEqual(len(sample_data), 6)
        self.assertTrue('libelle_acheminement' in sample_data[0])
        self.assertTrue('Paris 15e Arrondissement' in sample_data[1])
        self.assertTrue('2.29603434925049' in sample_data[1])
        self.assertTrue('48.845110357374' in sample_data[1])

    def test_single_fr_lareunion(self):
        ''' Test complete process_one.process on data that uses non-UTF8 encoding (issue #136)
        '''
        source = join(self.src_dir, u'fr/la-réunion.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))
            print(state_path, state)

        self.assertTrue(state['sample'] is not None)
        self.assertEqual(state['website'], 'http://adresse.data.gouv.fr/download/')
        self.assertIsNone(state['license'])

        with open(join(dirname(state_path), state['sample'])) as file:
            sample_data = json.load(file)

        self.assertEqual(len(sample_data), 6)
        self.assertTrue('libelle_acheminement' in sample_data[0])
        self.assertTrue('Saint-Joseph' in sample_data[1])
        self.assertTrue('55.6120442584072' in sample_data[1])
        self.assertTrue('-21.385871079156' in sample_data[1])

class TestPackage (unittest.TestCase):

    def test_package_output_csv(self):
        '''
        '''
        processed_csv = '/tmp/stuff.csv'
        website, license = 'http://ci.carson.ca.us/', 'Public domain'
        
        with mock.patch('zipfile.ZipFile') as ZipFile:
            package_output('us-ca-carson', processed_csv, website, license)

            self.assertEqual(len(ZipFile.return_value.mock_calls), 4)
            call1, call2, call3, call4 = ZipFile.return_value.mock_calls

        self.assertEqual(call1[0], 'writestr')
        self.assertEqual(call1[1][0], 'README.txt')
        readme_text = call1[1][1].decode('utf8')
        self.assertTrue(website in readme_text)
        self.assertTrue(license in readme_text)
        
        self.assertEqual(call2[0], 'writestr')
        self.assertEqual(call2[1][0], 'us-ca-carson.vrt')
        vrt_content = call2[1][1].decode('utf8')
        self.assertTrue('<OGRVRTLayer name="us-ca-carson">' in vrt_content)
        self.assertTrue('<SrcDataSource relativeToVRT="1">' in vrt_content)
        self.assertTrue('us-ca-carson.csv' in vrt_content)

        self.assertEqual(call3[0], 'write')
        self.assertEqual(call3[1][0], processed_csv)
        self.assertEqual(call3[1][1], 'us-ca-carson.csv')

        self.assertEqual(call4[0], 'close')

    def test_package_output_txt(self):
        '''
        '''
        processed_txt = '/tmp/stuff.txt'
        website, license = 'http://ci.carson.ca.us/', 'Public domain'
        
        with mock.patch('zipfile.ZipFile') as ZipFile:
            package_output('us-ca-carson', processed_txt, website, license)

            self.assertEqual(len(ZipFile.return_value.mock_calls), 3)
            call1, call2, call3 = ZipFile.return_value.mock_calls

        self.assertEqual(call1[0], 'writestr')
        self.assertEqual(call1[1][0], 'README.txt')
        readme_text = call1[1][1].decode('utf8')
        self.assertTrue(website in readme_text)
        self.assertTrue(license in readme_text)
        
        self.assertEqual(call2[0], 'write')
        self.assertEqual(call2[1][0], processed_txt)
        self.assertEqual(call2[1][1], 'us-ca-carson.txt')

        self.assertEqual(call3[0], 'close')

@contextmanager
def locked_open(filename):
    ''' Open and lock a file, for use with threads and processes.
    '''
    with open(filename, 'r+b') as file:
        if lockf:
            lockf(file, LOCK_EX)
        yield file
        if lockf:
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
