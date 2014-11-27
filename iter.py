import ijson, csv, pprint
from itertools import chain

with open('iter.csv', 'w') as output, open('iter.geojson') as input:
    out = csv.writer(output)
    for (i, (p, e, v)) in enumerate(ijson.parse(input)):
        out.writerow((i, p, e, v))

def build_value(data):
    ''' Build a value (number, array, whatever) from an ijson stream.
    '''
    for (prefix, event, value) in data:
        if event in ('string', 'number', 'null', 'boolean'):
            return value
        
        elif event == 'start_array':
            return build_list(data)
        
        elif event == 'start_map':
            return build_map(data)
        
        else:
            # MOOP.
            raise ValueError((prefix, event, value))

def build_list(data):
    ''' Build a list from an ijson stream.
    
        Stop when 'end_array' is reached.
    '''
    output = list()
    
    for (prefix, event, value) in data:
        if event == 'end_array':
            break
        
        else:
            # let build_value handle the array item.
            _data = chain([(prefix, event, value)], data)
            output.append(build_value(_data))
    
    return output        

def build_map(data):
    ''' Build a dictionary from an ijson stream.
    
        Stop when 'end_map' is reached.
    '''
    output = dict()

    for (prefix, event, value) in data:
        if event == 'end_map':
            break
        
        elif event == 'map_key':
            output[value] = build_value(data)
        
        else:
            # MOOP.
            raise ValueError((prefix, event, value))
    
    return output

file = open('iter.geojson')
data = ijson.parse(file)

pprint.pprint(build_value(data))

def sample_geojson(stream, max_features=2):
    '''
    '''
    features = list()
    data = ijson.parse(stream)

    for (prefix, event, value) in data:
        if event != 'start_map':
            # A root GeoJSON object is a map.
            raise ValueError((prefix, event, value))

        for (prefix, event, value) in data:
            if event == 'map_key' and value == 'type':
                prefix, event, value = data.next()
            
                if event != 'string' and value != 'FeatureCollection':
                    # We only want GeoJSON feature collections
                    raise ValueError((prefix, event, value))
            
            elif event == 'map_key' and value == 'features':
                prefix, event, value = data.next()
            
                if event != 'start_array':
                    # We only want lists of features here.
                    raise ValueError((prefix, event, value))
            
                for (prefix, event, value) in data:
                    if event == 'end_array' or len(features) == max_features:
                        break
                
                    # let build_value handle the feature.
                    _data = chain([(prefix, event, value)], data)
                    features.append(build_value(_data))

                return dict(type='FeatureCollection', features=features)
    
    raise ValueError()

file = open('iter.geojson')
print sample_geojson(file)

# http://s3.amazonaws.com/data.openaddresses.io/20141122/us-fl-palm_beach.json
import httplib

conn = httplib.HTTPConnection('s3.amazonaws.com')
conn.request('GET', '/data.openaddresses.io/20141122/us-fl-palm_beach.json')
resp = conn.getresponse()

pprint.pprint(sample_geojson(resp))
