import config
import utils

import fiona
import unittest
import csv

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
