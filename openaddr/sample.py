from __future__ import absolute_import, division, print_function
from .compat import standard_library

import json, ijson
from itertools import chain

def _build_value(data):
    ''' Build a value (number, array, whatever) from an ijson stream.
    '''
    for (prefix, event, value) in data:
        if event in ('string', 'null', 'boolean'):
            return value
        
        elif event == 'number':
            return int(value) if (int(value) == float(value)) else float(value)
        
        elif event == 'start_array':
            return _build_list(data)
        
        elif event == 'start_map':
            return _build_map(data)
        
        else:
            # MOOP.
            raise ValueError((prefix, event, value))

def _build_list(data):
    ''' Build a list from an ijson stream.
    
        Stop when 'end_array' is reached.
    '''
    output = list()
    
    for (prefix, event, value) in data:
        if event == 'end_array':
            break
        
        else:
            # let _build_value() handle the array item.
            _data = chain([(prefix, event, value)], data)
            output.append(_build_value(_data))
    
    return output        

def _build_map(data):
    ''' Build a dictionary from an ijson stream.
    
        Stop when 'end_map' is reached.
    '''
    output = dict()

    for (prefix, event, value) in data:
        if event == 'end_map':
            break
        
        elif event == 'map_key':
            output[value] = _build_value(data)
        
        else:
            # MOOP.
            raise ValueError((prefix, event, value))
    
    return output

def sample_geojson(stream, max_features):
    ''' Read a stream of input GeoJSON and return a string with a limited feature count.
    '''
    data, features = ijson.parse(stream), list()

    for (prefix1, event1, value1) in data:
        if event1 != 'start_map':
            # A root GeoJSON object is a map.
            raise ValueError((prefix1, event1, value1))

        for (prefix2, event2, value2) in data:
            if event2 == 'map_key' and value2 == 'type':
                prefix3, event3, value3 = next(data)
            
                if event3 != 'string' and value3 != 'FeatureCollection':
                    # We only want GeoJSON feature collections
                    raise ValueError((prefix3, event3, value3))
            
            elif event2 == 'map_key' and value2 == 'features':
                prefix4, event4, value4 = next(data)
            
                if event4 != 'start_array':
                    # We only want lists of features here.
                    raise ValueError((prefix4, event4, value4))
            
                for (prefix5, event5, value5) in data:
                    if event5 == 'end_array' or len(features) == max_features:
                        break
                
                    # let _build_value() handle the feature.
                    _data = chain([(prefix5, event5, value5)], data)
                    features.append(_build_value(_data))

                geojson = dict(type='FeatureCollection', features=features)
                return json.dumps(geojson)
    
    raise ValueError()
