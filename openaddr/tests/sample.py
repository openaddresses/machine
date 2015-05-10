from __future__ import absolute_import, division, print_function

import json
import unittest

from io import BytesIO

from ..sample import sample_geojson

class TestSample (unittest.TestCase):
    
    def test_sample(self):
        geojson_input = b'''{ "type": "FeatureCollection", "features": [
                            { "type": "Feature", "geometry": {"type": "Point", "coordinates": [102.0, 0.5]}, "properties": {"prop0": "value0"} },
                            { "type": "Feature", "geometry": { "type": "LineString", "coordinates": [ [102.0, 0.0], [103.0, 1.0], [104.0, 0.0], [105.0, 1.0] ] }, "properties": { "prop0": "value0", "prop1": 0.0 } },
                            { "type": "Feature", "geometry": { "type": "Polygon", "coordinates": [ [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ] ] }, "properties": { "prop0": "value0", "prop1": {"this": "that"}, "prop2": true, "prop3": null } }
                            ] }'''
        
        geojson0 = json.loads(sample_geojson(BytesIO(geojson_input), max_features=0))
        self.assertEqual(len(geojson0['features']), 0)
        
        geojson1 = json.loads(sample_geojson(BytesIO(geojson_input), max_features=1))
        self.assertEqual(len(geojson1['features']), 1)
        
        geojson2 = json.loads(sample_geojson(BytesIO(geojson_input), max_features=2))
        self.assertEqual(len(geojson2['features']), 2)
        
        geojson3 = json.loads(sample_geojson(BytesIO(geojson_input), max_features=3))
        self.assertEqual(len(geojson3['features']), 3)
        
        geojson4 = json.loads(sample_geojson(BytesIO(geojson_input), max_features=4))
        self.assertEqual(len(geojson4['features']), 3)

        self.assertEqual(geojson0['type'], 'FeatureCollection')

        self.assertEqual(geojson1['features'][0]['type'], 'Feature')
        self.assertEqual(geojson1['features'][0]['properties']['prop0'], 'value0')
        self.assertEqual(geojson1['features'][0]['geometry']['type'], 'Point')
        self.assertEqual(len(geojson1['features'][0]['geometry']['coordinates']), 2)
        self.assertEqual(geojson1['features'][0]['geometry']['coordinates'][0], 102.)
        self.assertEqual(geojson1['features'][0]['geometry']['coordinates'][1], .5)
        
        self.assertEqual(geojson2['features'][1]['geometry']['type'], 'LineString')
        self.assertEqual(len(geojson2['features'][1]['geometry']['coordinates']), 4)
        self.assertEqual(geojson2['features'][1]['geometry']['coordinates'][0][0], 102.)
        self.assertEqual(geojson2['features'][1]['geometry']['coordinates'][0][1], 0.)
        
        self.assertEqual(geojson3['features'][2]['geometry']['type'], 'Polygon')
        self.assertEqual(len(geojson3['features'][2]['geometry']['coordinates']), 1)
        self.assertEqual(geojson3['features'][2]['geometry']['coordinates'][0][0][0], 100.)
        self.assertEqual(geojson3['features'][2]['geometry']['coordinates'][0][0][1], 0.)
