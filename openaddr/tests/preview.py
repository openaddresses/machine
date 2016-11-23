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
            if url.hostname == 'tile.mapzen.com' and url.path.startswith('/mapzen/vector/v1'):
                if 'api_key=mapzen-XXXX' not in url.query:
                    raise ValueError('Missing or wrong API key')
                data = b'{"landuse": {"features": []}, "water": {"features": []}, "roads": {"features": []}}'
                return response(200, data, headers={'Content-Type': 'application/json'})
            raise Exception("Uknown URL")

        zip_filename = join(dirname(__file__), 'outputs', 'alameda.zip')
        handle, png_filename = tempfile.mkstemp(prefix='render-', suffix='.png')
        os.close(handle)

        try:
            with HTTMock(response_content):
                preview.render(zip_filename, png_filename, 668, 1, 'mapzen-XXXX')
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
            if url.hostname == 'tile.mapzen.com' and url.path.startswith('/mapzen/vector/v1'):
                if 'api_key=mapzen-XXXX' not in url.query:
                    raise ValueError('Missing or wrong API key')
                data = b'{"landuse": {"features": []}, "water": {"features": []}, "roads": {"features": []}}'
                return response(200, data, headers={'Content-Type': 'application/json'})
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
                preview.render(csv_filename, png_filename, 668, 1, 'mapzen-XXXX')
            info = str(subprocess.check_output(('file', png_filename)))

            self.assertTrue('PNG image data' in info)
            self.assertTrue('668 x 289' in info)
            self.assertTrue('8-bit/color RGB' in info)
        finally:
            os.remove(png_filename)
            os.remove(csv_filename)
            os.rmdir(temp_dir)
