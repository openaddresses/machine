from __future__ import division

import os
import unittest
import tempfile
import subprocess

from os.path import join, dirname

from httmock import HTTMock, response

from .. import preview

class TestPreview (unittest.TestCase):

    def test_stats(self):
        values = range(-1000, 1001)
        mean, stddev = preview.stats(values)
        self.assertEqual(mean, 0)
        self.assertAlmostEqual(stddev, 577.638872191)

    def test_calculate_bounds(self):
        points = [(-10000, -10000), (10000, 10000)]
        points += [(-1, -1), (0, 0), (1, 1)] * 100
        bbox = preview.calculate_bounds(points)
        self.assertEqual(bbox, (-1.04, -1.04, 1.04, 1.04), 'The two outliers are ignored')
    
    def test_render(self):
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
