#!/usr/bin/env python3
import csv
import os
import pprint
import requests
import zipfile
import fiona
from shapely.geometry import MultiPolygon, shape

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

  path = './workspace/{}'.format(idx)
  if not os.path.exists(path):
    os.makedirs(path)

  if not os.path.isfile(path +'/cache.zip'):
    cache_url = source[header.index('cache')]
    fetch(cache_url, path + '/cache.zip')

  if not os.path.exists(path + '/cached_files'):
    unzip(path + '/cache.zip', path + '/cached_files')

  with fiona.drivers():
    return fiona.open(path + '/cached_files/PARCELS.shp')

def geometry_stats():
  geometries = {}

  for row in state:
    geometry = row[header.index('geometry type')]
    try:
      geometries[geometry] += 1
    except KeyError:
      geometries[geometry] = 1

  print(geometries)

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

  f = parse_source(state[4], 4)
  objects = []
  for obj in f:  # this is a lossy process needed to get shapely to consume this data
    if 'geometry' in obj and obj['geometry']:  # needed for 'geometry': None
      if 'type' in obj['geometry']:
        if obj['geometry']['type'] == 'Polygon':
          objects.append(obj['geometry'])

  shapes = [shape(obj) for obj in objects]
  p = MultiPolygon(shapes)
  print(p)
