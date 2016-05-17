#!/usr/bin/env python3
import resource
import traceback
import csv
import os
import re
import pprint
import requests
import zipfile
import fiona
from shapely.geometry import MultiPolygon, shape, mapping
from shapely.wkt import dumps

extensions = ['shp', 'geojson']

"""Fetch directly to disk, low memory usage"""
def fetch(url, filepath):
  r = requests.get(url, stream=True)
  with open(filepath, 'wb') as f:
    for chunk in r.iter_content(chunk_size=1024): 
      if chunk: # filter out keep-alive new chunks
        f.write(chunk)
        f.flush()

  return filepath

def unzip(filepath, dest):
  with zipfile.ZipFile(filepath) as zf:
    zf.extractall(dest)

def parse_source(source, idx):
  print('parsing {} [{}]'.format(source[header.index('source')], idx))

  try:
    path = './workspace/{}'.format(idx)
    if not os.path.exists(path):
      os.makedirs(path)

    if not os.path.isfile(path +'/cache.zip'):
      cache_url = source[header.index('cache')]
      cache_filename = re.search('/[^/]*$', cache_url).group()
      fetch(cache_url, path + cache_filename)  # some of these are csv, not zip

    if not os.path.exists(path + '/cached_files'):
      unzip(path + '/cache.zip', path + '/cached_files')

    files = os.listdir(path + '/cached_files')
    shapefile = None
    for f in files:
      if re.match('.*\.({})$'.format('|'.join(extensions)), f):
        shapefile = f

    if shapefile and os.path.isfile(path + '/cached_files/{}'.format(shapefile)):
      with fiona.drivers():
        return fiona.open(path + '/cached_files/{}'.format(shapefile))
    else:
      return None
  except Exception as e:
    #print(e)
    return None

def geometry_stats():
  geometries = {}

  for row in state:
    geometry = row[header.index('geometry type')]
    try:
      geometries[geometry] += 1
    except KeyError:
      geometries[geometry] = 1

  print(geometries)

def convert_to_shapely(source):
  #print('mem3 {}'.format(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))
  '''
  objects = []
  for obj in f:  # this is a lossy process needed to get shapely to consume this data
    if 'geometry' in obj and obj['geometry']:  # needed for 'geometry': None
      if 'type' in obj['geometry']:
        if obj['geometry']['type'] == 'Polygon':
          objects.append(obj['geometry'])
  try:
    objects = f

    #print('mem4 {}'.format(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))
    shapes = [shape(obj) for obj in objects]
    #print('mem5 {}'.format(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))
    return MultiPolygon(shapes)
  except Exception as e:
    return None
  '''
  shapes = []

  for f in source:
    try:
      geom = shape(f['geometry'])
      if not geom.is_valid:
        clean = geom.buffer(0.0)
        assert clean.is_valid
        assert clean.geom_type == 'Polygon'
        geom = clean
      #f['geometry'] = mapping(geom)

      if geom.geom_type == 'Polygon':
        shapes.append(geom)

    except Exception as e:
      #print(e)
      #traceback.print_exc(file=sys.stdout)
      pass

  x = MultiPolygon(shapes)
  del shapes
  return x

if __name__=='__main__':
  state = []
  if not os.path.isfile('./state.txt'):
    fetch('http://results.openaddresses.io/state.txt', './state.txt')

  with open('state.txt', 'r') as statefile:
    statereader = csv.reader(statefile, delimiter='	')
    for row in statereader:
      state.append(row)

  header = state.pop(0)

  # purge non-polygon geometries from state
  to_purge = []

  for source in state:
    if 'Polygon' not in source[header.index('geometry type')]:
      to_purge.append(source)  # can't modify list during iteration


  for source in to_purge:
    state.remove(source)

  succ_count = 0

  for idx in range(0, len(state)):
    #print('mem0 {}'.format(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))
    f = parse_source(state[idx], idx)
    #print('mem1 {}'.format(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))
    if f:
      s = convert_to_shapely(f)
      if s:
        #print('mem6 {}'.format(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))
        #print(dumps(s))
        del s
        succ_count += 1
        print("{} passed ({}/{})".format(idx, succ_count, len(state)))
      else:
        print("{} failed [conversion]".format(idx))
      del f
    else:
      print("{} failed [parsing]".format(idx))
