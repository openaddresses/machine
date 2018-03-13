from __future__ import division

import os
import unittest
import tempfile
import subprocess

from os.path import join, dirname
from zipfile import ZipFile
from shutil import rmtree

from httmock import HTTMock, response

from .. import preview

class TestPreview (unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix='TestPreview-')

    def tearDown(self):
        rmtree(self.temp_dir)

    def test_stats(self):
        points = [(n, n) for n in range(-1000, 1001)]
        points_filename = join(self.temp_dir, 'points.bin')
        preview.write_points(points, points_filename)

        xmean, xsdev, ymean, ysdev = preview.stats(points_filename)
        self.assertAlmostEqual(xmean, 0)
        self.assertAlmostEqual(xsdev, 577.783263863)
        self.assertAlmostEqual(ymean, xmean)
        self.assertAlmostEqual(ysdev, xsdev)

    def test_calculate_bounds(self):
        points = [(-10000, -10000), (10000, 10000)]
        points += [(-1, -1), (0, 0), (1, 1)] * 100
        points_filename = join(self.temp_dir, 'points.bin')
        preview.write_points(points, points_filename)

        bbox = preview.calculate_bounds(points_filename)
        self.assertEqual(bbox, (-1.04, -1.04, 1.04, 1.04), 'The two outliers are ignored')

    def test_render_zip(self):
        '''
        '''
        def response_content(url, request):
            if url.hostname == 'a.tiles.mapbox.com' and url.path.startswith('/v4/mapbox.mapbox-streets-v7'):
                if 'access_token=mapbox-XXXX' not in url.query:
                    raise ValueError('Missing or wrong API key')
                data = b'\x1a\'x\x02\n\x05water(\x80 \x12\x19\x18\x03"\x13\t\xe0\x7f\xff\x1f\x1a\x00\xe0\x9f\x01\xdf\x9f\x01\x00\x00\xdf\x9f\x01\x0f\x08\x00'
                return response(200, data, headers={'Content-Type': 'application/vnd.mapbox-vector-tile'})
            raise Exception("Uknown URL")

        zip_filename = join(dirname(__file__), 'outputs', 'alameda.zip')
        handle, png_filename = tempfile.mkstemp(prefix='render-', suffix='.png')
        os.close(handle)

        try:
            with HTTMock(response_content):
                preview.render(zip_filename, png_filename, 668, 1, 'mapbox-XXXX')
            info = str(subprocess.check_output(('file', png_filename)))

            self.assertTrue('PNG image data' in info)
            self.assertTrue('668 x 573' in info)
            self.assertTrue('8-bit/color RGB' in info)
        finally:
            os.remove(png_filename)

    def test_render_csv(self):
        '''
        '''
        def response_content(url, request):
            if url.hostname == 'a.tiles.mapbox.com' and url.path.startswith('/v4/mapbox.mapbox-streets-v7'):
                if 'access_token=mapbox-XXXX' not in url.query:
                    raise ValueError('Missing or wrong API key')
                data = b'\x1a\'x\x02\n\x05water(\x80 \x12\x19\x18\x03"\x13\t\xe0\x7f\xff\x1f\x1a\x00\xe0\x9f\x01\xdf\x9f\x01\x00\x00\xdf\x9f\x01\x0f\x08\x00'
                return response(200, data, headers={'Content-Type': 'application/vnd.mapbox-vector-tile'})
            raise Exception("Uknown URL")

        zip_filename = join(dirname(__file__), 'outputs', 'portland_metro.zip')
        handle, png_filename = tempfile.mkstemp(prefix='render-', suffix='.png')
        os.close(handle)

        try:
            temp_dir = tempfile.mkdtemp(prefix='test_render_csv-')
            zipfile = ZipFile(zip_filename)

            with open(join(temp_dir, 'portland.csv'), 'wb') as file:
                file.write(zipfile.read('portland_metro/us/or/portland_metro.csv'))
                csv_filename = file.name

            with HTTMock(response_content):
                preview.render(csv_filename, png_filename, 668, 1, 'mapbox-XXXX')
            info = str(subprocess.check_output(('file', png_filename)))

            self.assertTrue('PNG image data' in info)
            self.assertTrue('668 x 289' in info)
            self.assertTrue('8-bit/color RGB' in info)
        finally:
            os.remove(png_filename)
            os.remove(csv_filename)
            os.rmdir(temp_dir)

    def test_get_map_features(self):
        '''
        '''
        def response_content(url, request):
            if url.hostname == 'a.tiles.mapbox.com' and url.path.startswith('/v4/mapbox.mapbox-streets-v7'):
                if 'access_token=mapbox-XXXX' not in url.query:
                    raise ValueError('Missing or wrong API key')
                with open(join(dirname(__file__), 'data', 'mapbox-tile.mvt'), 'rb') as file:
                    data = file.read()
                return response(200, data, headers={'Content-Type': 'application/vnd.mapbox-vector-tile'})
            raise Exception("Uknown URL")

        xmin, ymin, xmax, ymax = -13611952, 4551290, -13609564, 4553048
        scale = 100 / (xmax - xmin)

        with HTTMock(response_content):
            landuse_geoms, water_geoms, roads_geoms = \
                preview.get_map_features(xmin, ymin, xmax, ymax, 2, scale, 'mapbox-XXXX')

        self.assertEqual(len(landuse_geoms), 90, 'Should have 90 landuse geometries')
        self.assertEqual(len(water_geoms), 1, 'Should have 1 water geometry')
        self.assertEqual(len(roads_geoms), 792, 'Should have 792 road geometries')


