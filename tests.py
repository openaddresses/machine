import config
import utils

import fiona
import unittest

class TestUtils (unittest.TestCase):

    def setUp(self):
        config.openaddr_dir, self._prev_openaddr_dir = '.', config.openaddr_dir
    
    def tearDown(self):
        config.openaddr_dir, self._prev_openaddr_dir = self._prev_openaddr_dir, None
    
    def test_scrape_csv_metadata(self):
        header = ['OA:geom', 'APN', 'CONDO', 'POINT_X', 'POINT_Y', 'LocationID', 'StreetNum', 'Prequalifi', 'Direction', 'StreetName', 'StreetSufx', 'Unit', 'City', 'State', 'Zip', 'UseCode', 'UseCodeDes', 'BldgSqft', 'LotSqft', 'latitude', 'longitude', 'X_min', 'X_max', 'Y_min', 'Y_max', 'Shape_area', 'Shape_len']
        row = ['POLYGON ((565011.581500000320375 4190878.6357499994338,565007.068900000303984 4190904.882000001147389,565044.69565000012517 4190911.299,565049.508399999700487 4190885.2125,565011.581500000320375 4190878.6357499994338))', '055 183213100', 'YES', '565028.20427000', '4190894.98674000', '71843', '2550', '', '', 'DANA', 'ST', '', 'BERKELEY', 'CA', '94704', '7390', 'CONDOMINIUM-COMMON AREA', '0', '0', '37.86320476', '-122.26071359', '565007.06890000', '565049.50840000', '4190878.63575000', '4190911.29900000', '1018.77538590000', '129.82101701300']
        scraped = utils.scrape_csv_metadata(row, header, 'us/ca/berkeley.json')
        
        self.assertEqual(scraped, {'CITY': 'BERKELEY', 'ID': '055 183213100', 'REGION': None, 'STREET': 'DANA ST', 'NUMBER': '2550', 'DISTRICT': None, 'LON': None, 'LAT': None, 'UNIT': '', 'POSTCODE': '94704'})
    
    def test_scrape_fiona_metadata(self):
        data = fiona.open('data/us/ca/berkeley/Parcels.shp')
        obj = next(data)
        scraped = utils.scrape_fiona_metadata(obj, 'us/ca/berkeley.json')
        
        self.assertEqual(scraped, {'CITY': 'BERKELEY', 'ID': '055 183213100', 'REGION': None, 'STREET': 'DANA ST', 'NUMBER': '2550', 'DISTRICT': None, 'LON': None, 'LAT': None, 'UNIT': '', 'POSTCODE': '94704'})
