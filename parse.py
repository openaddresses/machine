#!/usr/bin/env python3
import csv
import os
import requests
import zipfile

state = []

with open('state.txt', 'r') as statefile:
  statereader = csv.reader(statefile, delimiter='	')
  for row in statereader:
    state.append(row)

header = state.pop(0)

# purge non-polygon geometries from state
###########################################################################
to_purge = []

for source in state:
  if 'Polygon' not in source[header.index('geometry type')]:
    to_purge.append(source)  # can't modify list during iteration


for source in to_purge:
  state.remove(source)

###########################################################################
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

def parse_source(idx):
  print('parsing {} [{}]'.format(state[idx][header.index('source')], idx))
  path = './workspace/{}'.format(idx)
  if not os.path.exists(path):
    os.makedirs(path)

  # Download cache file
  cache_url = state[idx][header.index('cache')]
  fetch(cache_url, path + '/cache.zip')
  unzip(path + '/cache.zip', path + '/cached_files')
  # What metadata do we need to differentiate different 'polygon' sources? Where is/will that be stored?

  import shapefile
  sf = shapefile.Reader(path + '/cached_files/PARCELS')

  print(sf.shapes())

def geometry_stats():
  geometries = {}

  for row in state:
    geometry = row[header.index('geometry type')]
    try:
      geometries[geometry] += 1
    except KeyError:
      geometries[geometry] = 1

  print(geometries)

###########################################################################

parse_source(4)
