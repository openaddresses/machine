import config
import utils

import unittest, csv
from collections import OrderedDict

import fiona
import mock

class TestUtils (unittest.TestCase):

    def setUp(self):
        config.openaddr_dir, self._prev_openaddr_dir = '.', config.openaddr_dir
    
    def tearDown(self):
        config.openaddr_dir, self._prev_openaddr_dir = self._prev_openaddr_dir, None
    
    def test_scrape_csv_metadata(self):
        with open('data/us/ca/berkeley/Parcels.csv') as file:
            rows = csv.reader(file)
            header, row = next(rows), next(rows)
            scraped = utils.scrape_csv_metadata(row, header, 'us/ca/berkeley.json')
        
        self.assertEqual(scraped, {'CITY': 'BERKELEY', 'ID': '055 183213100', 'REGION': None, 'STREET': 'DANA ST', 'NUMBER': '2550', 'DISTRICT': None, 'LON': None, 'LAT': None, 'UNIT': '', 'POSTCODE': '94704'})
    
    def test_scrape_fiona_metadata(self):
        with fiona.open('data/us/ca/berkeley/Parcels.shp') as data:
            obj = next(data)
            scraped = utils.scrape_fiona_metadata(obj, 'us/ca/berkeley.json')
        
        self.assertEqual(scraped, {'CITY': 'BERKELEY', 'ID': '055 183213100', 'REGION': None, 'STREET': 'DANA ST', 'NUMBER': '2550', 'DISTRICT': None, 'LON': None, 'LAT': None, 'UNIT': '', 'POSTCODE': '94704'})
    
    def test_to_shapely_obj(self):
        with fiona.open('data/us/ca/berkeley/Parcels.shp') as data:
            obj = next(data)
            shaped = utils.to_shapely_obj(obj)
        
        self.assertEqual(str(shaped), 'POLYGON ((565011.5815000003 4190878.635749999, 565007.0689000003 4190904.882000001, 565044.6956500001 4190911.299000001, 565049.5083999997 4190885.2125, 565011.5815000003 4190878.635749999))')
    
    def test_import_csv(self):
        with mock.patch('utils.scrape_csv_metadata') as scrape_csv_metadata:
            imported = utils.import_csv('data/us/ca/berkeley/Parcels.csv', 'us/ca/berkeley.json')
        
        args = [call[1] for call in scrape_csv_metadata.mock_calls if call[0] == '']
        self.assertEqual(args[0], (['POLYGON ((565011.581500000320375 4190878.6357499994338,565007.068900000303984 4190904.882000001147389,565044.69565000012517 4190911.299,565049.508399999700487 4190885.2125,565011.581500000320375 4190878.6357499994338))', '055 183213100', 'YES', '565028.20427000', '4190894.98674000', '71843', '2550', '', '', 'DANA', 'ST', '', 'BERKELEY', 'CA', '94704', '7390', 'CONDOMINIUM-COMMON AREA', '0', '0', '37.86320476', '-122.26071359', '565007.06890000', '565049.50840000', '4190878.63575000', '4190911.29900000', '1018.77538590000', '129.82101701300'], ['OA:geom', 'APN', 'CONDO', 'POINT_X', 'POINT_Y', 'LocationID', 'StreetNum', 'Prequalifi', 'Direction', 'StreetName', 'StreetSufx', 'Unit', 'City', 'State', 'Zip', 'UseCode', 'UseCodeDes', 'BldgSqft', 'LotSqft', 'latitude', 'longitude', 'X_min', 'X_max', 'Y_min', 'Y_max', 'Shape_area', 'Shape_len'], 'us/ca/berkeley.json'))
        self.assertEqual(args[1], (['POLYGON ((562519.81180000025779 4190028.839099999517,562504.675150000490248 4190024.9286,562496.860450000502169 4190054.97375,562511.5466499999166 4190058.6435,562518.123399999924 4190034.966299999505,562519.81180000025779 4190028.839099999517))', '053 166004000', 'YES', '562508.24304500', '4190041.76403000', '67007', '1012', '', '', 'GRAYSON', 'ST', 'A', 'BERKELEY', 'CA', '94710', '4200', 'INDUSTRIAL LIGHT/MANUFAC.', '1932', '1667', '37.85569154', '-122.28943405', '562496.86045000', '562519.81180000', '4190024.92860000', '4190058.64350000', '476.85359785500', '92.74539444770'], ['OA:geom', 'APN', 'CONDO', 'POINT_X', 'POINT_Y', 'LocationID', 'StreetNum', 'Prequalifi', 'Direction', 'StreetName', 'StreetSufx', 'Unit', 'City', 'State', 'Zip', 'UseCode', 'UseCodeDes', 'BldgSqft', 'LotSqft', 'latitude', 'longitude', 'X_min', 'X_max', 'Y_min', 'Y_max', 'Shape_area', 'Shape_len'], 'us/ca/berkeley.json'))
        self.assertEqual(args[2], (['POLYGON ((562519.81180000025779 4190028.839099999517,562504.675150000490248 4190024.9286,562496.860450000502169 4190054.97375,562511.5466499999166 4190058.6435,562518.123399999924 4190034.966299999505,562519.81180000025779 4190028.839099999517))', '053 166004200', 'YES', '562508.24304500', '4190041.76403000', '67009', '1012', '', '', 'GRAYSON', 'ST', 'C', 'BERKELEY', 'CA', '94710', '4101', 'CONDOMINIUM-INDUSTRIAL', '1932', '1667', '37.85569154', '-122.28943405', '562496.86045000', '562519.81180000', '4190024.92860000', '4190058.64350000', '476.85359785500', '92.74539444770'], ['OA:geom', 'APN', 'CONDO', 'POINT_X', 'POINT_Y', 'LocationID', 'StreetNum', 'Prequalifi', 'Direction', 'StreetName', 'StreetSufx', 'Unit', 'City', 'State', 'Zip', 'UseCode', 'UseCodeDes', 'BldgSqft', 'LotSqft', 'latitude', 'longitude', 'X_min', 'X_max', 'Y_min', 'Y_max', 'Shape_area', 'Shape_len'], 'us/ca/berkeley.json'))
        self.assertEqual(len(args), 3)
    
    def test_import_with_fiona(self):
        with mock.patch('utils.scrape_fiona_metadata') as scrape_fiona_metadata:
            imported = utils.import_with_fiona('data/us/ca/berkeley/Parcels.shp', 'us/ca/berkeley.json')
        
        args = [call[1] for call in scrape_fiona_metadata.mock_calls if call[0] == '']
        self.assertEqual(args[0], ({'geometry': {'type': 'Polygon', 'coordinates': [[(565011.5815000003, 4190878.6357499994), (565007.0689000003, 4190904.882000001), (565044.6956500001, 4190911.2990000006), (565049.5083999997, 4190885.2125000004), (565011.5815000003, 4190878.6357499994)]]}, 'type': 'Feature', 'id': '0', 'properties': OrderedDict([('APN', '055 183213100'), ('CONDO', 'YES'), ('POINT_X', 565028.20427), ('POINT_Y', 4190894.98674), ('LocationID', 71843.0), ('StreetNum', 2550.0), ('Prequalifi', None), ('Direction', None), ('StreetName', 'DANA'), ('StreetSufx', 'ST'), ('Unit', None), ('City', 'BERKELEY'), ('State', 'CA'), ('Zip', '94704'), ('UseCode', '7390'), ('UseCodeDes', 'CONDOMINIUM-COMMON AREA'), ('BldgSqft', 0.0), ('LotSqft', 0.0), ('latitude', 37.86320476), ('longitude', -122.26071359), ('X_min', 565007.0689), ('X_max', 565049.5084), ('Y_min', 4190878.63575), ('Y_max', 4190911.299), ('Shape_area', 1018.7753859), ('Shape_len', 129.821017013)])}, 'us/ca/berkeley.json'))
        self.assertEqual(args[1], ({'geometry': {'type': 'Polygon', 'coordinates': [[(562519.8118000003, 4190028.8390999995), (562504.6751500005, 4190024.9286), (562496.8604500005, 4190054.973750001), (562511.5466499999, 4190058.6435000002), (562518.1233999999, 4190034.9662999995), (562519.8118000003, 4190028.8390999995)]]}, 'type': 'Feature', 'id': '1', 'properties': OrderedDict([('APN', '053 166004000'), ('CONDO', 'YES'), ('POINT_X', 562508.243045), ('POINT_Y', 4190041.76403), ('LocationID', 67007.0), ('StreetNum', 1012.0), ('Prequalifi', None), ('Direction', None), ('StreetName', 'GRAYSON'), ('StreetSufx', 'ST'), ('Unit', 'A'), ('City', 'BERKELEY'), ('State', 'CA'), ('Zip', '94710'), ('UseCode', '4200'), ('UseCodeDes', 'INDUSTRIAL LIGHT/MANUFAC.'), ('BldgSqft', 1932.0), ('LotSqft', 1667.0), ('latitude', 37.85569154), ('longitude', -122.28943405), ('X_min', 562496.86045), ('X_max', 562519.8118), ('Y_min', 4190024.9286), ('Y_max', 4190058.6435), ('Shape_area', 476.853597855), ('Shape_len', 92.7453944477)])}, 'us/ca/berkeley.json'))
        self.assertEqual(args[2], ({'geometry': {'type': 'Polygon', 'coordinates': [[(562519.8118000003, 4190028.8390999995), (562504.6751500005, 4190024.9286), (562496.8604500005, 4190054.973750001), (562511.5466499999, 4190058.6435000002), (562518.1233999999, 4190034.9662999995), (562519.8118000003, 4190028.8390999995)]]}, 'type': 'Feature', 'id': '2', 'properties': OrderedDict([('APN', '053 166004200'), ('CONDO', 'YES'), ('POINT_X', 562508.243045), ('POINT_Y', 4190041.76403), ('LocationID', 67009.0), ('StreetNum', 1012.0), ('Prequalifi', None), ('Direction', None), ('StreetName', 'GRAYSON'), ('StreetSufx', 'ST'), ('Unit', 'C'), ('City', 'BERKELEY'), ('State', 'CA'), ('Zip', '94710'), ('UseCode', '4101'), ('UseCodeDes', 'CONDOMINIUM-INDUSTRIAL'), ('BldgSqft', 1932.0), ('LotSqft', 1667.0), ('latitude', 37.85569154), ('longitude', -122.28943405), ('X_min', 562496.86045), ('X_max', 562519.8118), ('Y_min', 4190024.9286), ('Y_max', 4190058.6435), ('Shape_area', 476.853597855), ('Shape_len', 92.7453944477)])}, 'us/ca/berkeley.json'))
        self.assertEqual(len(args), 3)
