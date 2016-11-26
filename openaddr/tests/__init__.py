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
import os
import csv
from os import close, environ, mkdir, remove
from io import BytesIO
from csv import DictReader
from itertools import cycle
from zipfile import ZipFile
from datetime import timedelta
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
        
from .. import (
    cache, conform, S3, process_one,
    iterate_local_processed_files, download_processed_file
    )

from ..util import package_output
from ..ci.objects import Run, RunState
from ..cache import CacheResult
from ..conform import ConformResult

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
        
        if host == 'tile.mapzen.com' and path.startswith('/mapzen/vector/v1'):
            if 'api_key=mapzen-XXXX' not in url.query:
                raise ValueError('Missing or wrong API key')
            data = b'{"landuse": {"features": []}, "water": {"features": []}, "roads": {"features": []}}'
            return response(200, data, headers={'Content-Type': 'application/json'})

        if (host, path) == ('data.acgov.org', '/api/geospatial/8e4s-7f4v'):
            local_path = join(data_dirname, 'us-ca-alameda_county-excerpt.zip')
        
        if (host, path) == ('data.acgov.org', '/api/geospatial/MiXeD-cAsE'):
            local_path = join(data_dirname, 'us-ca-alameda_county-excerpt-mixedcase.zip')
        
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

        if (host, path) == ('ftp.vgingis.com', '/Download/VA_SiteAddress.txt.zip'):
            local_path = join(data_dirname, 'VA_SiteAddress-excerpt.zip')
        
        if (host, path) == ('gis3.oit.ohio.gov', '/LBRS/_downloads/TRU_ADDS.zip'):
            local_path = join(data_dirname, 'TRU_ADDS-excerpt.zip')
        
        if (host, path) == ('s3.amazonaws.com', '/data.openaddresses.io/cache/uploads/iandees/ed482f/bucks.geojson.zip'):
            local_path = join(data_dirname, 'us-pa-bucks.geojson.zip')
        
        if (host, path) == ('www.carsonproperty.info', '/ArcGIS/rest/services/basemap/MapServer/1/query'):
            qs = parse_qs(query)
            body_data = parse_qs(request.body) if request.body else {}

            if qs.get('returnIdsOnly') == ['true']:
                local_path = join(data_dirname, 'us-ca-carson-ids-only.json')
            elif qs.get('returnCountOnly') == ['true']:
                local_path = join(data_dirname, 'us-ca-carson-count-only.json')
            elif body_data.get('outSR') == ['4326']:
                local_path = join(data_dirname, 'us-ca-carson-0.json')

        if (host, path) == ('www.carsonproperty.info', '/ArcGIS/rest/services/basemap/MapServer/1'):
            qs = parse_qs(query)

            if qs.get('f') == ['json']:
                local_path = join(data_dirname, 'us-ca-carson-metadata.json')

        if (host, path) == ('72.205.198.131', '/ArcGIS/rest/services/Brown/Brown/MapServer/33/query'):
            qs = parse_qs(query)
            body_data = parse_qs(request.body) if request.body else {}

            if qs.get('returnIdsOnly') == ['true']:
                local_path = join(data_dirname, 'us-ks-brown-ids-only.json')
            elif qs.get('returnCountOnly') == ['true']:
                local_path = join(data_dirname, 'us-ks-brown-count-only.json')
            elif body_data.get('outSR') == ['4326']:
                local_path = join(data_dirname, 'us-ks-brown-0.json')

        if (host, path) == ('72.205.198.131', '/ArcGIS/rest/services/Brown/Brown/MapServer/33'):
            qs = parse_qs(query)

            if qs.get('f') == ['json']:
                local_path = join(data_dirname, 'us-ks-brown-metadata.json')

        if (host, path) == ('services1.arcgis.com', '/I6XnrlnguPDoEObn/arcgis/rest/services/AddressPoints/FeatureServer/0/query'):
            qs = parse_qs(query)
            body_data = parse_qs(request.body) if request.body else {}

            if qs.get('returnCountOnly') == ['true']:
                local_path = join(data_dirname, 'us-pa-lancaster-count-only.json')
            elif body_data.get('outSR') == ['4326']:
                local_path = join(data_dirname, 'us-pa-lancaster-0.json')
            elif body_data.get('resultRecordCount') == ['1']:
                local_path = join(data_dirname, 'us-pa-lancaster-probe.json')

        if (host, path) == ('services1.arcgis.com', '/I6XnrlnguPDoEObn/arcgis/rest/services/AddressPoints/FeatureServer/0'):
            qs = parse_qs(query)

            if qs.get('f') == ['json']:
                local_path = join(data_dirname, 'us-pa-lancaster-metadata.json')

        if (host, path) == ('maps.co.washington.mn.us', '/arcgis/rest/services/Public/Public_Parcels/MapServer/0/query'):
            qs = parse_qs(query)
            body_data = parse_qs(request.body) if request.body else {}

            if qs.get('returnCountOnly') == ['true']:
                local_path = join(data_dirname, 'us-nm-washington-count-only.json')
            elif body_data.get('outSR') == ['4326']:
                local_path = join(data_dirname, 'us-nm-washington-0.json')
            elif body_data.get('resultRecordCount') == ['1']:
                local_path = join(data_dirname, 'us-nm-washington-probe.json')

        if (host, path) == ('maps.co.washington.mn.us', '/arcgis/rest/services/Public/Public_Parcels/MapServer/0'):
            qs = parse_qs(query)

            if qs.get('f') == ['json']:
                local_path = join(data_dirname, 'us-nm-washington-metadata.json')

        if (host, path) == ('gis.ci.waco.tx.us', '/arcgis/rest/services/Parcels/MapServer/0/query'):
            qs = parse_qs(query)
            body_data = parse_qs(request.body) if request.body else {}

            if qs.get('returnCountOnly') == ['true']:
                local_path = join(data_dirname, 'us-tx-waco-count-only.json')
            elif body_data.get('outSR') == ['4326']:
                local_path = join(data_dirname, 'us-tx-waco-0.json')

        if (host, path) == ('gis.ci.waco.tx.us', '/arcgis/rest/services/Parcels/MapServer/0'):
            qs = parse_qs(query)

            if qs.get('f') == ['json']:
                local_path = join(data_dirname, 'us-tx-waco-metadata.json')

        if (host, path) == ('data.openaddresses.io', '/20000101/us-ca-carson-cached.json'):
            local_path = join(data_dirname, 'us-ca-carson-cache.geojson')
        
        if (host, path) == ('data.openaddresses.io', '/cache/fr/BAN_licence_gratuite_repartage_75.zip'):
            local_path = join(data_dirname, 'BAN_licence_gratuite_repartage_75.zip')
        
        if (host, path) == ('data.openaddresses.io', '/cache/fr/BAN_licence_gratuite_repartage_974.zip'):
            local_path = join(data_dirname, 'BAN_licence_gratuite_repartage_974.zip')
        
        if (host, path) == ('fbarc.stadt-berlin.de', '/FIS_Broker_Atom/Hauskoordinaten/HKO_EPSG3068.zip'):
            local_path = join(data_dirname, 'de-berlin-excerpt.zip')
        
        if (host, path) == ('www.dropbox.com', '/s/8uaqry2w657p44n/bagadres.zip'):
            local_path = join(data_dirname, 'nl.zip')
        
        if (host, path) == ('fake-web', '/lake-man.gdb.zip'):
            local_path = join(data_dirname, 'lake-man.gdb.zip')
        
        if (host, path) == ('fake-web', '/lake-man-gdb-othername.zip'):
            local_path = join(data_dirname, 'lake-man-gdb-othername.zip')
        
        if (host, path) == ('fake-web', '/lake-man-gdb-othername-nodir.zip'):
            local_path = join(data_dirname, 'lake-man-gdb-othername-nodir.zip')
        
        if scheme == 'file':
            local_path = path

        if local_path:
            type, _ = guess_type(local_path)
            with open(local_path, 'rb') as file:
                return response(200, file.read(), headers={'Content-Type': type})
        
        raise NotImplementedError(url.geturl())
    
    def response_content_ftp(self, url):
        ''' Fake FTP responses for use with mock.patch in tests.
        '''
        scheme, host, path, _, _, _ = urlparse(url)
        data_dirname = join(dirname(__file__), 'data')
        local_path = None
        
        if scheme != 'ftp':
            raise ValueError("Don't know how to {}".format(scheme))
        
        if (host, path) == ('ftp.agrc.utah.gov', '/UtahSGID_Vector/UTM12_NAD83/LOCATION/UnpackagedData/AddressPoints/_Statewide/AddressPoints_shp.zip'):
            local_path = join(data_dirname, 'us-ut-excerpt.zip')
        
        if (host, path) == ('ftp02.portlandoregon.gov', '/CivicApps/address.zip'):
            local_path = join(data_dirname, 'us-or-portland.zip')

        if (host, path) == ('ftp.skra.is', '/skra/STADFANG.dsv.zip'):
            local_path = join(data_dirname, 'iceland.zip')

        if local_path:
            type, _ = guess_type(local_path)
            with open(local_path, 'rb') as file:
                return response(200, file.read(), headers={'Content-Type': type})
        
        raise NotImplementedError(url)
        
    def test_single_ac(self):
        ''' Test complete process_one.process on Alameda County sample data.
        '''
        source = join(self.src_dir, 'us-ca-alameda_county.json')
        
        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, True, mapzen_key='mapzen-XXXX')
        
        with open(state_path) as file:
            state = dict(zip(*json.load(file)))
        
        self.assertIsNotNone(state['cache'])
        self.assertIsNotNone(state['processed'])
        self.assertIsNotNone(state['sample'])
        self.assertIsNotNone(state['preview'])
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
            self.assertEqual(rows[1]['STREET'], 'BROADWAY')
            self.assertEqual(rows[10]['STREET'], 'HILLSBOROUGH ST')
            self.assertEqual(rows[100]['STREET'], '8TH ST')
            self.assertEqual(rows[1000]['STREET'], 'HANOVER AV')
            self.assertEqual(rows[1]['UNIT'], '')
            self.assertEqual(rows[10]['UNIT'], '')
            self.assertEqual(rows[100]['UNIT'], '')
            self.assertEqual(rows[1000]['UNIT'], '')

    def test_single_ac_mixedcase(self):
        ''' Test complete process_one.process on Alameda County sample data.
        '''
        source = join(self.src_dir, 'us-ca-alameda_county-mixedcase.json')
        
        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, True, mapzen_key='mapzen-XXXX')
        
        with open(state_path) as file:
            state = dict(zip(*json.load(file)))
        
        self.assertIsNotNone(state['cache'])
        self.assertIsNotNone(state['processed'])
        self.assertIsNotNone(state['sample'])
        self.assertIsNotNone(state['preview'])
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
            self.assertEqual(rows[1]['STREET'], 'BROADWAY')
            self.assertEqual(rows[10]['STREET'], 'HILLSBOROUGH ST')
            self.assertEqual(rows[100]['STREET'], '8TH ST')
            self.assertEqual(rows[1000]['STREET'], 'HANOVER AV')

    def test_single_sf(self):
        ''' Test complete process_one.process on San Francisco sample data.
        '''
        source = join(self.src_dir, 'us-ca-san_francisco.json')
        
        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, True, mapzen_key='mapzen-XXXX')
        
        with open(state_path) as file:
            state = dict(zip(*json.load(file)))
        
        self.assertIsNotNone(state['cache'])
        self.assertIsNotNone(state['processed'])
        self.assertIsNotNone(state['sample'])
        self.assertIsNotNone(state['preview'])
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
            self.assertEqual(rows[1]['STREET'], 'OCTAVIA ST')
            self.assertEqual(rows[10]['STREET'], 'GOLDEN GATE AVE')
            self.assertEqual(rows[100]['STREET'], 'OCTAVIA ST')
            self.assertEqual(rows[1000]['STREET'], 'FOLSOM ST')
            self.assertEqual(rows[1]['UNIT'], '')
            self.assertEqual(rows[10]['UNIT'], '')
            self.assertEqual(rows[100]['UNIT'], '')
            self.assertEqual(rows[1000]['UNIT'], '')

    def test_single_car(self):
        ''' Test complete process_one.process on Carson sample data.
        '''
        source = join(self.src_dir, 'us-ca-carson.json')
        
        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, True, mapzen_key='mapzen-XXXX')
        
        with open(state_path) as file:
            state = dict(zip(*json.load(file)))
        
        self.assertIsNotNone(state['cache'])
        self.assertEqual(state['fingerprint'], 'ef20174833d33c4ea50451a0b8a2d7f3')
        self.assertIsNotNone(state['processed'])
        self.assertIsNotNone(state['sample'])
        self.assertIsNotNone(state['preview'])
        self.assertEqual(state['geometry type'], 'Point')
        self.assertEqual(state['website'], 'http://ci.carson.ca.us/')
        self.assertIsNone(state['license'])
        
        with open(join(dirname(state_path), state['sample'])) as file:
            sample_data = json.load(file)
        
        self.assertEqual(len(sample_data), 6)
        self.assertTrue('SITENUMBER' in sample_data[0])
        
        with open(join(dirname(state_path), state['processed'])) as file:
            rows = list(DictReader(file, dialect='excel'))
            self.assertEqual(5, len(rows))
            self.assertEqual(rows[0]['NUMBER'], '555')
            self.assertEqual(rows[0]['STREET'], 'CARSON ST')
            self.assertEqual(rows[0]['UNIT'], '')
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
            state_path = process_one.process(source, self.testdir, False)
        
        with open(state_path) as file:
            state = dict(zip(*json.load(file)))
        
        self.assertIsNotNone(state['cache'])
        self.assertEqual(state['fingerprint'], 'aff4e5c82562533c6e44adb5cf87103c')
        self.assertIsNotNone(state['processed'])
        self.assertIsNotNone(state['sample'])
        self.assertIsNone(state['preview'])
        self.assertEqual(state['geometry type'], 'Point')

        with open(join(dirname(state_path), state['sample'])) as file:
            sample_data = json.load(file)
        
        self.assertEqual(len(sample_data), 6)
        self.assertTrue('SITENUMBER' in sample_data[0])
        
        with open(join(dirname(state_path), state['processed'])) as file:
            self.assertTrue('555,CARSON ST' in file.read())

    def test_single_car_old_cached(self):
        ''' Test complete process_one.process on Carson sample data.
        '''
        source = join(self.src_dir, 'us-ca-carson-old-cached.json')
        
        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, False)
        
        with open(state_path) as file:
            state = dict(zip(*json.load(file)))
        
        self.assertIsNotNone(state['cache'])
        self.assertEqual(state['fingerprint'], 'aff4e5c82562533c6e44adb5cf87103c')
        self.assertIsNotNone(state['processed'])
        self.assertIsNotNone(state['sample'])
        self.assertIsNone(state['preview'])
        self.assertEqual(state['geometry type'], 'Point')

        with open(join(dirname(state_path), state['sample'])) as file:
            sample_data = json.load(file)
        
        self.assertEqual(len(sample_data), 6)
        self.assertTrue('SITENUMBER' in sample_data[0])
        
        with open(join(dirname(state_path), state['processed'])) as file:
            self.assertTrue('555,CARSON ST' in file.read())

    def test_single_oak(self):
        ''' Test complete process_one.process on Oakland sample data.
        '''
        source = join(self.src_dir, 'us-ca-oakland.json')
        
        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, False)
        
        with open(state_path) as file:
            state = RunState(dict(zip(*json.load(file))))
        
        self.assertFalse(state.skipped)
        self.assertIsNotNone(state.cache)
        # This test data does not contain a working conform object
        self.assertEqual(state.fail_reason, 'Unknown source conform type')
        self.assertIsNone(state.processed)
        self.assertIsNone(state.preview)
        self.assertEqual(state.website, 'http://data.openoakland.org/dataset/property-parcels/resource/df20b818-0d16-4da8-a9c1-a7b8b720ff49')
        self.assertIsNone(state.license)
        
        with open(join(dirname(state_path), state.sample)) as file:
            sample_data = json.load(file)
        
        self.assertTrue('FID_PARCEL' in sample_data[0])

    def test_single_oak_skip(self):
        ''' Test complete process_one.process on Oakland sample data.
        '''
        source = join(self.src_dir, 'us-ca-oakland-skip.json')
        
        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, False)
        
        with open(state_path) as file:
            state = dict(zip(*json.load(file)))
        
        # This test data says "skip": True
        self.assertTrue(state['skipped'])
        self.assertIsNone(state['cache'])
        self.assertIsNone(state['processed'])
        self.assertIsNone(state['preview'])

    def test_single_berk(self):
        ''' Test complete process_one.process on Berkeley sample data.
        '''
        source = join(self.src_dir, 'us-ca-berkeley.json')
        
        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, False)
        
        with open(state_path) as file:
            state = dict(zip(*json.load(file)))
        
        self.assertIsNotNone(state['cache'])
        # This test data does not contain a conform object at all
        self.assertIsNone(state['processed'])
        self.assertIsNone(state['preview'])
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
            state_path = process_one.process(source, self.testdir, False)
        
        with open(state_path) as file:
            state = dict(zip(*json.load(file)))
        
        self.assertIsNone(state['cache'])
        self.assertIsNone(state['processed'])
        self.assertIsNone(state['preview'])
        
    def test_single_berk_apn(self):
        ''' Test complete process_one.process on Berkeley sample data.
        '''
        source = join(self.src_dir, 'us-ca-berkeley-apn.json')
        
        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, False)
        
        with open(state_path) as file:
            state = dict(zip(*json.load(file)))
        
        self.assertIsNotNone(state['cache'])
        self.assertIsNotNone(state['processed'])
        self.assertIsNone(state['preview'])
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
            self.assertEqual(rows[1]['STREET'], 'DANA ST')
            self.assertEqual(rows[10]['STREET'], 'CHANNING WAY')
            self.assertEqual(rows[100]['STREET'], 'DURANT AVE')
            self.assertEqual(rows[1]['UNIT'], u'')
            self.assertEqual(rows[10]['UNIT'], u'')
            self.assertEqual(rows[100]['UNIT'], u'')

    def test_single_pl_ds(self):
        ''' Test complete process_one.process on Polish sample data.
        '''
        source = join(self.src_dir, 'pl-dolnoslaskie.json')
        
        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, False)
        
        with open(state_path) as file:
            state = dict(zip(*json.load(file)))
        
        self.assertIsNotNone(state['cache'])
        self.assertIsNotNone(state['processed'])
        self.assertIsNotNone(state['sample'])
        self.assertIsNone(state['preview'])
        self.assertEqual(state['geometry type'], 'Point')
        self.assertIsNone(state['website'])
        self.assertEqual(state['license'][:21], 'Polish Law on Geodesy')
        self.assertEqual(state['share-alike'], 'false')
        self.assertIn('issues/187#issuecomment-63327973', state['license'])
        
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
            state_path = process_one.process(source, self.testdir, False)
        
        with open(state_path) as file:
            state = dict(zip(*json.load(file)))
        
        self.assertIsNotNone(state['cache'])
        self.assertIsNotNone(state['processed'])
        self.assertIsNotNone(state['sample'])
        self.assertIsNone(state['preview'])
        self.assertEqual(state['geometry type'], 'Point')
        self.assertIsNone(state['website'])
        self.assertEqual(state['license'][:21], 'Polish Law on Geodesy')
        self.assertEqual(state['share-alike'], 'false')
        self.assertIn('issues/187#issuecomment-63327973', state['license'])
        
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
            self.assertEqual(rows[100]['STREET'], u'Plac pl. Inwalid\xf3w Wojennych  Gliwice')
            self.assertEqual(rows[1]['UNIT'], u'')
            self.assertEqual(rows[10]['UNIT'], u'')
            self.assertEqual(rows[100]['UNIT'], u'')

    def test_single_jp_f(self):
        ''' Test complete process_one.process on Japanese sample data.
        '''
        source = join(self.src_dir, 'jp-fukushima.json')
        
        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, False)
        
        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNotNone(state['sample'])
        self.assertIsNone(state['preview'])
        self.assertEqual(state['website'], 'http://nlftp.mlit.go.jp/isj/index.html')
        self.assertEqual(state['license'], u'http://nlftp.mlit.go.jp/ksj/other/yakkan§.html')
        self.assertEqual(state['attribution required'], 'true')
        self.assertIn('Ministry of Land', state['attribution name'])
        
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

        with mock.patch('openaddr.util.request_ftp_file', new=self.response_content_ftp):
            state_path = process_one.process(source, self.testdir, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNotNone(state['sample'])
        self.assertIsNone(state['preview'])
        self.assertIsNone(state['website'])
        self.assertIsNone(state['license'])

        with open(join(dirname(state_path), state['sample'])) as file:
            sample_data = json.load(file)

        self.assertEqual(len(sample_data), 6)

    def test_single_iceland(self):
        ''' Test complete process_one.process.
        '''
        source = join(self.src_dir, 'iceland.json')

        with mock.patch('openaddr.util.request_ftp_file', new=self.response_content_ftp):
            state_path = process_one.process(source, self.testdir, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNone(state['preview'])
        self.assertIsNotNone(state['processed'])
        self.assertIsNotNone(state['cache'])
        self.assertIsNotNone(state['sample'])
        self.assertIsNotNone(state['website'])
        self.assertIsNotNone(state['license'])

        with open(join(dirname(state_path), state['sample'])) as file:
            sample_data = json.load(file)

        self.assertEqual(len(sample_data), 6)
        row1 = dict(zip(sample_data[0], sample_data[1]))
        row2 = dict(zip(sample_data[0], sample_data[2]))
        row3 = dict(zip(sample_data[0], sample_data[3]))
        row4 = dict(zip(sample_data[0], sample_data[4]))
        self.assertEqual(row1['HEITI_NF'], u'2.Gata v/Rauðavatn')
        self.assertEqual(row2['GAGNA_EIGN'], u'Þjóðskrá Íslands')
        self.assertEqual(row3['LONG_WGS84'], '-21,76846217953')
        self.assertEqual(row4['LAT_WGS84'], '64,110044369942')

        with csvopen(join(dirname(state_path), state['processed']), encoding='utf8') as file:
            rows = list(csvDictReader(file, encoding='utf8'))
            
        self.assertEqual(len(rows), 15)
        self.assertEqual(rows[0]['STREET'], u'2.Gata v/Rauðavatn')
        self.assertAlmostEqual(float(rows[2]['LON']), -21.76846217953)
        self.assertAlmostEqual(float(rows[3]['LAT']), 64.110044369942)

    def test_single_fr_paris(self):
        ''' Test complete process_one.process on data that uses conform csvsplit (issue #124)
        '''
        source = join(self.src_dir, 'fr-paris.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNotNone(state['sample'])
        self.assertIsNone(state['preview'])
        self.assertEqual(state['website'], 'http://adresse.data.gouv.fr/download/')
        self.assertIsNone(state['license'])
        self.assertEqual(state['attribution required'], 'true')
        self.assertEqual(state['share-alike'], 'true')
        self.assertIn(u'Géographique et Forestière', state['attribution name'])

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
            state_path = process_one.process(source, self.testdir, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNotNone(state['sample'])
        self.assertIsNone(state['preview'])
        self.assertEqual(state['website'], 'http://adresse.data.gouv.fr/download/')
        self.assertIsNone(state['license'])
        self.assertEqual(state['attribution required'], 'true')
        self.assertEqual(state['share-alike'], 'true')
        self.assertIn(u'Géographique et Forestière', state['attribution name'])

        with open(join(dirname(state_path), state['sample'])) as file:
            sample_data = json.load(file)

        self.assertEqual(len(sample_data), 6)
        self.assertTrue('libelle_acheminement' in sample_data[0])
        self.assertTrue('Saint-Joseph' in sample_data[1])
        self.assertTrue('55.6120442584072' in sample_data[1])
        self.assertTrue('-21.385871079156' in sample_data[1])

    def test_single_va_statewide(self):
        ''' Test complete process_one.process on data with non-OGR .csv filename.
        '''
        source = join(self.src_dir, 'us/va/statewide.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNotNone(state['sample'])
        self.assertIsNone(state['preview'])

        with open(join(dirname(state_path), state['sample'])) as file:
            sample_data = json.load(file)

        self.assertEqual(len(sample_data), 6)
        self.assertTrue('ADDRNUM' in sample_data[0])
        self.assertTrue('393' in sample_data[1])
        self.assertTrue('36.596097285069824' in sample_data[1])
        self.assertTrue('-81.260533627271982' in sample_data[1])

    def test_single_oh_trumbull(self):
        ''' Test complete process_one.process on data with .txt filename present.
        '''
        source = join(self.src_dir, 'us/oh/trumbull.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNotNone(state['sample'])
        self.assertIsNone(state['preview'])

        with open(join(dirname(state_path), state['sample'])) as file:
            sample_data = json.load(file)

        self.assertEqual(len(sample_data), 6)
        self.assertTrue('HOUSENUM' in sample_data[0])
        self.assertTrue(775 in sample_data[1])
        self.assertTrue(2433902.038 in sample_data[1])
        self.assertTrue(575268.364 in sample_data[1])

    def test_single_ks_brown(self):
        ''' Test complete process_one.process on data with ESRI multiPolyline geometries.
        '''
        source = join(self.src_dir, 'us/ks/brown_county.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNotNone(state['sample'])
        self.assertIsNone(state['preview'])

        with open(join(dirname(state_path), state['sample'])) as file:
            sample_data = json.load(file)

        self.assertEqual(len(sample_data), 6)
        self.assertTrue('OA:geom' in sample_data[0])

    def test_single_pa_lancaster(self):
        ''' Test complete process_one.process on data with ESRI multiPolyline geometries.
        '''
        source = join(self.src_dir, 'us/pa/lancaster.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNotNone(state['sample'])
        self.assertIsNone(state['preview'])

        with open(join(dirname(state_path), state['sample'])) as file:
            sample_data = json.load(file)

        self.assertEqual(len(sample_data), 6)
        self.assertIn('OA:geom', sample_data[0])
        self.assertIn('UNITNUM', sample_data[0])
        self.assertEqual('423', sample_data[1][0])
        self.assertEqual(['W', ' ', '28TH DIVISION', 'HWY'], sample_data[1][1:5])
        self.assertEqual('1', sample_data[1][6])
        self.assertEqual('2', sample_data[2][6])
        self.assertEqual('3', sample_data[3][6])
        self.assertEqual('4', sample_data[4][6])
        self.assertEqual('5', sample_data[5][6])
        
        output_path = join(dirname(state_path), state['processed'])
        
        with csvopen(output_path, encoding='utf8') as input:
            rows = list(csvDictReader(input, encoding='utf8'))
            self.assertEqual(rows[1]['UNIT'], u'2')
            self.assertEqual(rows[11]['UNIT'], u'11')
            self.assertEqual(rows[21]['UNIT'], u'')
            self.assertEqual(rows[1]['NUMBER'], u'423')
            self.assertEqual(rows[11]['NUMBER'], u'423')
            self.assertEqual(rows[21]['NUMBER'], u'7')
            self.assertEqual(rows[1]['STREET'], u'W 28TH DIVISION HWY')
            self.assertEqual(rows[11]['STREET'], u'W 28TH DIVISION HWY')
            self.assertEqual(rows[21]['STREET'], u'W 28TH DIVISION HWY')

    def test_single_pa_bucks(self):
        ''' Test complete process_one.process on data with ESRI multiPolyline geometries.
        '''
        source = join(self.src_dir, 'us/pa/bucks.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNotNone(state['sample'])
        self.assertIsNone(state['preview'])

        with open(join(dirname(state_path), state['sample'])) as file:
            sample_data = json.load(file)
            row1 = dict(zip(sample_data[0], sample_data[1]))
            row2 = dict(zip(sample_data[0], sample_data[2]))

        self.assertEqual(len(sample_data), 6)
        self.assertIn('SITUS_ADDR_NUM', sample_data[0])
        self.assertIn('MUNI', sample_data[0])
        self.assertEqual('', row1['SITUS_ADDR_NUM'])
        self.assertEqual('STATE', row1['SITUS_FNAME'])
        self.assertEqual('RD', row1['SITUS_FTYPE'])
        self.assertEqual('', row2['SITUS_ADDR_NUM'])
        self.assertEqual('STATE', row2['SITUS_FNAME'])
        self.assertEqual('RD', row2['SITUS_FTYPE'])
        
        output_path = join(dirname(state_path), state['processed'])
        
        with csvopen(output_path, encoding='utf8') as input:
            rows = list(csvDictReader(input, encoding='utf8'))
            self.assertEqual(rows[1]['UNIT'], u'')
            self.assertEqual(rows[10]['UNIT'], u'')
            self.assertEqual(rows[20]['UNIT'], u'')
            self.assertEqual(rows[1]['NUMBER'], u'')
            self.assertEqual(rows[10]['NUMBER'], u'')
            self.assertEqual(rows[20]['NUMBER'], u'429')
            self.assertEqual(rows[1]['STREET'], u'STATE RD')
            self.assertEqual(rows[10]['STREET'], u'STATE RD')
            self.assertEqual(rows[20]['STREET'], u'WALNUT AVE E')

    def test_single_nm_washington(self):
        ''' Test complete process_one.process on data without ESRI support for resultRecordCount.
        '''
        source = join(self.src_dir, 'us/nm/washington.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNotNone(state['sample'])
        self.assertIsNone(state['preview'])

        with open(join(dirname(state_path), state['sample'])) as file:
            sample_data = json.load(file)

        self.assertEqual(len(sample_data), 6)
        self.assertIn('OA:geom', sample_data[0])
        self.assertIn('BLDG_NUM', sample_data[0])
        self.assertEqual('7710', sample_data[1][0])
        self.assertEqual([' ', 'IVERSON', 'AVE', 'S'], sample_data[1][3:7])
        self.assertEqual('7710', sample_data[1][0])
        self.assertEqual('9884', sample_data[2][0])
        self.assertEqual('9030', sample_data[3][0])
        self.assertEqual('23110', sample_data[4][0])
        self.assertEqual(' ', sample_data[5][0])
        
        output_path = join(dirname(state_path), state['processed'])
        
        with csvopen(output_path, encoding='utf8') as input:
            rows = list(csvDictReader(input, encoding='utf8'))
            self.assertEqual(rows[1]['UNIT'], u'')
            self.assertEqual(rows[5]['UNIT'], u'')
            self.assertEqual(rows[9]['UNIT'], u'')
            self.assertEqual(rows[1]['NUMBER'], u'9884')
            self.assertEqual(rows[5]['NUMBER'], u'3842')
            self.assertEqual(rows[9]['NUMBER'], u'')
            self.assertEqual(rows[1]['STREET'], u'5TH STREET LN N')
            self.assertEqual(rows[5]['STREET'], u'ABERCROMBIE LN')
            self.assertEqual(rows[9]['STREET'], u'')

    def test_single_tx_waco(self):
        ''' Test complete process_one.process on data without ESRI support for resultRecordCount.
        '''
        source = join(self.src_dir, 'us/tx/city_of_waco.json')

        with HTTMock(self.response_content):
            ofs = csv.field_size_limit()
            csv.field_size_limit(1)
            state_path = process_one.process(source, self.testdir, False)
            csv.field_size_limit(ofs)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNone(state['sample'], 'Sample should be missing when csv.field_size_limit() is too short')

        source = join(self.src_dir, 'us/tx/city_of_waco.json')

        with HTTMock(self.response_content):
            ofs = csv.field_size_limit()
            csv.field_size_limit(sys.maxsize)
            state_path = process_one.process(source, self.testdir, False)
            csv.field_size_limit(ofs)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNotNone(state['sample'], 'Sample should be present when csv.field_size_limit() is long enough')
        self.assertIsNone(state['preview'])

        output_path = join(dirname(state_path), state['processed'])
        
        with csvopen(output_path, encoding='utf8') as input:
            rows = list(csvDictReader(input, encoding='utf8'))
            self.assertEqual(rows[0]['REGION'], u'TX')
            self.assertEqual(rows[0]['ID'], u'')
            self.assertEqual(rows[0]['NUMBER'], u'308')
            self.assertEqual(rows[0]['HASH'], u'0b0395441e3477b7')
            self.assertEqual(rows[0]['CITY'], u'Mcgregor')
            self.assertEqual(rows[0]['LON'], u'-97.3961771')
            self.assertEqual(rows[0]['LAT'], u'31.4432703')
            self.assertEqual(rows[0]['STREET'], u'PULLEN ST')
            self.assertEqual(rows[0]['POSTCODE'], u'76657')
            self.assertEqual(rows[0]['UNIT'], u'')
            self.assertEqual(rows[0]['DISTRICT'], u'')

    def test_single_de_berlin(self):
        ''' Test complete process_one.process on data.
        '''
        source = join(self.src_dir, 'de/berlin.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        output_path = join(dirname(state_path), state['processed'])
        
        with csvopen(output_path, encoding='utf8') as input:
            rows = list(csvDictReader(input, encoding='utf8'))
            self.assertEqual(rows[0]['NUMBER'], u'72')
            self.assertEqual(rows[1]['NUMBER'], u'3')
            self.assertEqual(rows[2]['NUMBER'], u'75')
            self.assertEqual(rows[0]['STREET'], u'Otto-Braun-Stra\xdfe')
            self.assertEqual(rows[1]['STREET'], u'Dorotheenstra\xdfe')
            self.assertEqual(rows[2]['STREET'], u'Alte Jakobstra\xdfe')

        self.assertIsNotNone(state['sample'])
        self.assertIsNone(state['preview'])

        with open(join(dirname(state_path), state['sample'])) as file:
            sample_data = json.load(file)

        self.assertEqual(len(sample_data), 6)
        
        for (sample_datum, row) in zip(sample_data[1:], rows[0:]):
            self.assertEqual(sample_datum[9], row['NUMBER'])
            self.assertEqual(sample_datum[13], row['STREET'])

    def test_single_us_or_portland(self):
        ''' Test complete process_one.process on data.
        '''
        source = join(self.src_dir, 'us/or/portland.json')

        with mock.patch('openaddr.util.request_ftp_file', new=self.response_content_ftp):
            state_path = process_one.process(source, self.testdir, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        output_path = join(dirname(state_path), state['processed'])
        
        with csvopen(output_path, encoding='utf8') as input:
            rows = list(csvDictReader(input, encoding='utf8'))
            self.assertEqual(len(rows), 12)
            self.assertEqual(rows[2]['NUMBER'], u'1')
            self.assertEqual(rows[3]['NUMBER'], u'10')
            self.assertEqual(rows[-2]['NUMBER'], u'2211')
            self.assertEqual(rows[-1]['NUMBER'], u'2211')
            self.assertEqual(rows[2]['STREET'], u'SW RICHARDSON ST')
            self.assertEqual(rows[3]['STREET'], u'SW PORTER ST')
            self.assertEqual(rows[-2]['STREET'], u'SE OCHOCO ST')
            self.assertEqual(rows[-1]['STREET'], u'SE OCHOCO ST')
            self.assertTrue(bool(rows[2]['LAT']))
            self.assertTrue(bool(rows[2]['LON']))
            self.assertTrue(bool(rows[3]['LAT']))
            self.assertTrue(bool(rows[3]['LON']))
            self.assertFalse(bool(rows[-2]['LAT']))
            self.assertFalse(bool(rows[-2]['LON']))
            self.assertTrue(bool(rows[-1]['LAT']))
            self.assertTrue(bool(rows[-1]['LON']))

    def test_single_nl_countrywide(self):
        ''' Test complete process_one.process on data.
        '''
        source = join(self.src_dir, 'nl/countrywide.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        output_path = join(dirname(state_path), state['processed'])
        
        with csvopen(output_path, encoding='utf8') as input:
            rows = list(csvDictReader(input, encoding='utf8'))
            self.assertEqual(len(rows), 8)
            self.assertEqual(rows[0]['NUMBER'], u'34x')
            self.assertEqual(rows[1]['NUMBER'], u'65-x')
            self.assertEqual(rows[2]['NUMBER'], u'147x-x')
            self.assertEqual(rows[3]['NUMBER'], u'6')
            self.assertEqual(rows[4]['NUMBER'], u'279b')
            self.assertEqual(rows[5]['NUMBER'], u'10')
            self.assertEqual(rows[6]['NUMBER'], u'601')
            self.assertEqual(rows[7]['NUMBER'], u'2')

    def test_single_lake_man_gdb(self):
        ''' Test complete process_one.process on data.
        '''
        source = join(self.src_dir, 'lake-man-gdb.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNotNone(state['sample'])
        self.assertIsNone(state['preview'])
        
        with open(join(dirname(state_path), state['sample'])) as file:
            sample_data = json.load(file)
        
        self.assertEqual(len(sample_data), 6)
        self.assertTrue('ADDRESSID' in sample_data[0])
        self.assertTrue(964 in sample_data[1])
        self.assertTrue('FRUITED PLAINS LN' in sample_data[1])
        
        output_path = join(dirname(state_path), state['processed'])
        
        with csvopen(output_path, encoding='utf8') as input:
            rows = list(csvDictReader(input, encoding='utf8'))
            self.assertEqual(len(rows), 6)
            self.assertEqual(rows[0]['NUMBER'], '5115')
            self.assertEqual(rows[0]['STREET'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[1]['NUMBER'], '5121')
            self.assertEqual(rows[1]['STREET'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[2]['NUMBER'], '5133')
            self.assertEqual(rows[2]['STREET'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[3]['NUMBER'], '5126')
            self.assertEqual(rows[3]['STREET'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[4]['NUMBER'], '5120')
            self.assertEqual(rows[4]['STREET'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[5]['NUMBER'], '5115')
            self.assertEqual(rows[5]['STREET'], 'OLD MILL RD')

    def test_single_lake_man_gdb_nested(self):
        ''' Test complete process_one.process on data.
        '''
        source = join(self.src_dir, 'lake-man-gdb-nested.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNotNone(state['sample'])
        self.assertIsNone(state['preview'])
        
        with open(join(dirname(state_path), state['sample'])) as file:
            sample_data = json.load(file)
        
        self.assertEqual(len(sample_data), 6)
        self.assertTrue('ADDRESSID' in sample_data[0])
        self.assertTrue(964 in sample_data[1])
        self.assertTrue('FRUITED PLAINS LN' in sample_data[1])
        
        output_path = join(dirname(state_path), state['processed'])
        
        with csvopen(output_path, encoding='utf8') as input:
            rows = list(csvDictReader(input, encoding='utf8'))
            self.assertEqual(len(rows), 6)
            self.assertEqual(rows[0]['NUMBER'], '5115')
            self.assertEqual(rows[0]['STREET'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[1]['NUMBER'], '5121')
            self.assertEqual(rows[1]['STREET'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[2]['NUMBER'], '5133')
            self.assertEqual(rows[2]['STREET'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[3]['NUMBER'], '5126')
            self.assertEqual(rows[3]['STREET'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[4]['NUMBER'], '5120')
            self.assertEqual(rows[4]['STREET'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[5]['NUMBER'], '5115')
            self.assertEqual(rows[5]['STREET'], 'OLD MILL RD')

    def test_single_lake_man_gdb_nested_nodir(self):
        ''' Test complete process_one.process on data.
        '''
        source = join(self.src_dir, 'lake-man-gdb-nested-nodir.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNotNone(state['sample'])
        self.assertIsNone(state['preview'])
        
        with open(join(dirname(state_path), state['sample'])) as file:
            sample_data = json.load(file)
        
        self.assertEqual(len(sample_data), 6)
        self.assertTrue('ADDRESSID' in sample_data[0])
        self.assertTrue(964 in sample_data[1])
        self.assertTrue('FRUITED PLAINS LN' in sample_data[1])
        
        output_path = join(dirname(state_path), state['processed'])
        
        with csvopen(output_path, encoding='utf8') as input:
            rows = list(csvDictReader(input, encoding='utf8'))
            self.assertEqual(len(rows), 6)
            self.assertEqual(rows[0]['NUMBER'], '5115')
            self.assertEqual(rows[0]['STREET'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[1]['NUMBER'], '5121')
            self.assertEqual(rows[1]['STREET'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[2]['NUMBER'], '5133')
            self.assertEqual(rows[2]['STREET'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[3]['NUMBER'], '5126')
            self.assertEqual(rows[3]['STREET'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[4]['NUMBER'], '5120')
            self.assertEqual(rows[4]['STREET'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[5]['NUMBER'], '5115')
            self.assertEqual(rows[5]['STREET'], 'OLD MILL RD')

class TestState (unittest.TestCase):
    
    def setUp(self):
        '''
        '''
        self.output_dir = tempfile.mkdtemp(prefix='TestState-')
    
    def tearDown(self):
        '''
        '''
        shutil.rmtree(self.output_dir)
    
    def test_write_state(self):
        '''
        '''
        log_handler = mock.Mock()

        with open(join(self.output_dir, 'log-handler-stream.txt'), 'w') as file:
            log_handler.stream.name = file.name
        
        with open(join(self.output_dir, 'processed.zip'), 'w') as file:
            processed_path = file.name

        with open(join(self.output_dir, 'preview.png'), 'w') as file:
            preview_path = file.name

        conform_result = ConformResult(processed=None, sample='/tmp/sample.json',
                                       website='http://example.com', license='ODbL',
                                       geometry_type='Point', address_count=999,
                                       path=processed_path, elapsed=timedelta(seconds=1),
                                       attribution_flag=True, attribution_name='Example',
                                       sharealike_flag=True)

        cache_result = CacheResult(cache='http://example.com/cache.csv',
                                   fingerprint='ff9900', version='0.0.0',
                                   elapsed=timedelta(seconds=2))
        
        #
        # Check result of process_one.write_state().
        #
        args = dict(source='sources/foo.json', skipped=False,
                    destination=self.output_dir, log_handler=log_handler,
                    cache_result=cache_result, conform_result=conform_result,
                    temp_dir=self.output_dir, preview_path=preview_path)

        path1 = process_one.write_state(**args)
        
        with open(path1) as file:
            state1 = dict(zip(*json.load(file)))
        
        self.assertEqual(state1['source'], 'foo.json')
        self.assertEqual(state1['skipped'], False)
        self.assertEqual(state1['cache'], 'http://example.com/cache.csv')
        self.assertEqual(state1['sample'], 'sample.json')
        self.assertEqual(state1['website'], 'http://example.com')
        self.assertEqual(state1['license'], 'ODbL')
        self.assertEqual(state1['geometry type'], 'Point')
        self.assertEqual(state1['address count'], 999)
        self.assertEqual(state1['version'], '0.0.0')
        self.assertEqual(state1['fingerprint'], 'ff9900')
        self.assertEqual(state1['cache time'], '0:00:02')
        self.assertEqual(state1['processed'], 'out.zip')
        self.assertEqual(state1['process time'], '0:00:01')
        self.assertEqual(state1['output'], 'output.txt')
        self.assertEqual(state1['preview'], 'preview.png')
        self.assertEqual(state1['share-alike'], 'true')
        self.assertEqual(state1['attribution required'], 'true')
        self.assertEqual(state1['attribution name'], 'Example')

        #
        # Tweak a few values, try process_one.write_state() again.
        #
        conform_result.attribution_flag = False

        args.update(source='sources/foo/bar.json', skipped=True)
        path2 = process_one.write_state(**args)
        
        with open(path2) as file:
            state2 = dict(zip(*json.load(file)))

        self.assertEqual(state2['source'], 'bar.json')
        self.assertEqual(state2['skipped'], True)
        self.assertEqual(state2['attribution required'], 'false')

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

    def test_iterate_local_processed_files(self):
        state0 = {'processed': 'http://s3.amazonaws.com/openaddresses/000.csv'}
        state1 = {'processed': 'http://s3.amazonaws.com/openaddresses/123.csv', 'website': 'http://example.com'}
        state3 = {'processed': 'http://s3.amazonaws.com/openaddresses/789.csv', 'license': 'ODbL'}
    
        runs = [
            Run(000, 'sources/000.json', '___', b'', None,
                RunState(state0), None, None, None, None, None, None, None, None),
            Run(123, 'sources/123.json', 'abc', b'', None,
                RunState(state1), None, None, None, None, None, None, None, None),
            Run(456, 'sources/456.json', 'def', b'', None,
                RunState({'processed': None}), None, None, None, None, None, None, None, None),
            Run(789, 'sources/7/9.json', 'ghi', b'', None,
                RunState(state3), None, None, None, None, None, None, None, None),
            ]
        
        failure = cycle((True, True, False))
        
        def _download_processed_file(url):
            if url == state0['processed']:
                raise Exception('HTTP 404 Not Found')
            elif next(failure):
                raise Exception('HTTP 666 Transient B.S.')
            else:
                return 'nonexistent file'

        with mock.patch('openaddr.download_processed_file') as download_processed_file:
            download_processed_file.side_effect = _download_processed_file
            local_processed_files = iterate_local_processed_files(runs)
            
            local_processed_result1 = next(local_processed_files)
            local_processed_result2 = next(local_processed_files)
            
            self.assertEqual(local_processed_result1.source_base, '123')
            self.assertEqual(local_processed_result1.filename, 'nonexistent file')
            self.assertEqual(local_processed_result1.run_state.processed, state1['processed'])
            self.assertEqual(local_processed_result2.source_base, '7/9')
            self.assertEqual(local_processed_result2.filename, 'nonexistent file')
            self.assertEqual(local_processed_result2.run_state.processed, state3['processed'])
            self.assertEqual(local_processed_result2.run_state.license, state3['license'])

    def response_content(self, url, request):
        '''
        '''
        MHP = request.method, url.hostname, url.path
        
        if MHP == ('GET', 's3.amazonaws.com', '/openaddresses/us-oh-clinton.csv'):
            return response(200, b'...', headers={'Last-Modified': 'Wed, 30 Apr 2014 17:42:10 GMT'})
        
        if MHP == ('GET', 'data.openaddresses.io.s3.amazonaws.com', '/runs/11170/ca-ab-strathcona-county.zip'):
            return response(200, b'...', headers={'Last-Modified': 'Tue, 18 Aug 2015 07:10:32 GMT'})
        
        if MHP == ('GET', 'data.openaddresses.io.s3.amazonaws.com', '/runs/13616/fr/vaucluse.zip'):
            return response(200, b'...', headers={'Last-Modified': 'Wed, 19 Aug 2015 10:35:44 GMT'})

        raise ValueError(url.geturl())
        
    def test_download_processed_file_csv(self):
        with HTTMock(self.response_content):
            filename = download_processed_file('http://s3.amazonaws.com/openaddresses/us-oh-clinton.csv')

        self.assertEqual(splitext(filename)[1], '.csv')
        self.assertEqual(os.stat(filename).st_mtime, 1398879730)
        remove(filename)

    def test_download_processed_file_zip(self):
        with HTTMock(self.response_content):
            filename = download_processed_file('http://data.openaddresses.io.s3.amazonaws.com/runs/11170/ca-ab-strathcona-county.zip')

        self.assertEqual(splitext(filename)[1], '.zip')
        self.assertEqual(os.stat(filename).st_mtime, 1439881832)
        remove(filename)

    def test_download_processed_file_nested_zip(self):
        with HTTMock(self.response_content):
            filename = download_processed_file('http://data.openaddresses.io.s3.amazonaws.com/runs/13616/fr/vaucluse.zip')

        self.assertEqual(splitext(filename)[1], '.zip')
        self.assertEqual(os.stat(filename).st_mtime, 1439980544)
        remove(filename)

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
    md5 = b'0xDEADBEEF'
    
    def __init__(self, name, fake_s3):
        self.name = name
        self.s3 = fake_s3
    
    def generate_url(self, **kwargs):
        return 'http://fake-s3.local' + self.name

    def set_contents_from_string(self, string, **kwargs):
        self.s3._write_fake_key(self.name, string)
        
    def set_contents_from_filename(self, filename, **kwargs):
        with open(filename, 'rb') as file:
            self.s3._write_fake_key(self.name, file.read())
