from shutil import rmtree
from os.path import join
from tempfile import mkdtemp
from zipfile import ZipFile
import unittest

import mock

from ..dotmap import get_all_features

class TestDotmap (unittest.TestCase):

    def setUp(self):
        self.test_dir = mkdtemp()
        self.zipfiles = list()
        
        self.zipfiles.append(join(self.test_dir, 'file1.zip'))
        zf = ZipFile(self.zipfiles[-1], 'w')
        zf.writestr('README.txt', 'Good times')
        zf.writestr('stuff.csv', 'LAT,LON\n0,0\n37.804319,-122.271210\n')
        zf.close()
        
        self.zipfiles.append(join(self.test_dir, 'file2.zip'))
        zf = ZipFile(self.zipfiles[-1], 'w')
        zf.writestr('README.txt', 'Good times')
        zf.writestr('stuff.csv', 'LON,LAT\n0,0\n-122.413729,37.775641\n')
        zf.close()
    
    def tearDown(self):
        rmtree(self.test_dir)

    def test_get_all_features_no_runs(self):
        features = list(get_all_features(self.zipfiles[:0]))
        self.assertEqual(len(features), 0)

    def test_get_all_features_one_run(self):
        features = list(get_all_features(self.zipfiles[:1]))

        p1, p2 = [f['geometry']['coordinates'] for f in features]
        self.assertAlmostEqual(p1[0],    0.0)
        self.assertAlmostEqual(p1[1],    0.0)
        self.assertAlmostEqual(p2[0], -122.271210)
        self.assertAlmostEqual(p2[1],   37.804319)

    def test_get_all_features_two_runs(self):
        features = list(get_all_features(self.zipfiles[:2]))

        p1, p2, p3, p4 = [f['geometry']['coordinates'] for f in features]
        self.assertAlmostEqual(p1[0],    0.0)
        self.assertAlmostEqual(p1[1],    0.0)
        self.assertAlmostEqual(p2[0], -122.271210)
        self.assertAlmostEqual(p2[1],   37.804319)
        self.assertAlmostEqual(p3[0],    0.0)
        self.assertAlmostEqual(p3[1],    0.0)
        self.assertAlmostEqual(p4[0], -122.413729)
        self.assertAlmostEqual(p4[1],   37.775641)
