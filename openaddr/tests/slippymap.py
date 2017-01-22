from __future__ import division

import os
import unittest
import tempfile
import mock

from os.path import join, dirname
from zipfile import ZipFile
from shutil import rmtree

from .. import slippymap

class TestSlippyMap (unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix='TestPreview-')
    
    def tearDown(self):
        rmtree(self.temp_dir)

    def test_render_zip(self):
        '''
        '''
        zip_filename = join(dirname(__file__), 'outputs', 'alameda.zip')
        handle, mbtiles_filename = tempfile.mkstemp(prefix='render-', suffix='.mbtiles')
        os.close(handle)

        try:
            with mock.patch('subprocess.Popen') as Popen:
                slippymap.generate(zip_filename, mbtiles_filename)

            self.assertEqual(len(Popen.return_value.stdin.write.mock_calls), 2 * 5305)
            self.assertEqual(len(Popen.return_value.stdin.close.mock_calls), 1)
            self.assertEqual(Popen.mock_calls[0][1][0],
                ('tippecanoe', '-l', 'dots', '-r', '3', '-n', 'OpenAddresses Dots',
                '-f', '-t', tempfile.gettempdir(), '-o', mbtiles_filename))
        finally:
            os.remove(mbtiles_filename)
    
    def test_render_csv(self):
        '''
        '''
        zip_filename = join(dirname(__file__), 'outputs', 'portland_metro.zip')
        handle, mbtiles_filename = tempfile.mkstemp(prefix='render-', suffix='.mbtiles')
        os.close(handle)

        try:
            temp_dir = tempfile.mkdtemp(prefix='test_render_csv-')
            zipfile = ZipFile(zip_filename)
            
            with open(join(temp_dir, 'portland.csv'), 'wb') as file:
                file.write(zipfile.read('portland_metro/us/or/portland_metro.csv'))
                csv_filename = file.name
        
            with mock.patch('subprocess.Popen') as Popen:
                slippymap.generate(csv_filename, mbtiles_filename)

            self.assertEqual(len(Popen.return_value.stdin.write.mock_calls), 2 * 767)
            self.assertEqual(len(Popen.return_value.stdin.close.mock_calls), 1)
            self.assertEqual(Popen.mock_calls[0][1][0],
                ('tippecanoe', '-l', 'dots', '-r', '3', '-n', 'OpenAddresses Dots',
                '-f', '-t', tempfile.gettempdir(), '-o', mbtiles_filename))
        finally:
            os.remove(mbtiles_filename)
            os.remove(csv_filename)
            os.rmdir(temp_dir)
