import csv
import json
import fiona
import os
import requests
import sys
import zipfile
import traceback

import config

from shapely.geometry import shape
from shapely.wkt import loads, dumps
from openaddr.conform import conform_smash_case, row_transform_and_convert

csv.field_size_limit(sys.maxsize)


def fetch(url, filepath):
    """Get a file from a URL

    Writes directly to disk, without caching the whole file in
    RAM.
    """
    r = requests.get(url, stream=True)
    with open(filepath, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
                f.flush()

    return filepath


def unzip(filepath, dest):
    """Unzip $filepath to $dest"""
    with zipfile.ZipFile(filepath) as zf:
        zf.extractall(dest)


def rlistdir(path):
    """Get all files from $path recursively.

    Does not follow symlinks.
    """
    files = []
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            files.append(os.path.join(dirpath, f))

    return files


def to_shapely_obj(data):
    """Return a clean shapely object.

    Accepts a fiona shape object and converts it to shapely.
    """
    if 'geometry' in data and data['geometry']:
        geom = shape(data['geometry'])
        if config.clean_geom:
            if not geom.is_valid:  # sends warnings to stderr
                clean = geom.buffer(0.0)  # attempt to clean shape
                assert clean.is_valid
                geom = clean

        if geom.geom_type == 'Polygon':
            return geom

    return None


def scrape_fiona_metadata(obj, source):
    """What does this data look like?

    """

    source_json = json.loads(open('{}/sources/{}'.format(config.openaddr_dir, source)).read())
    cleaned_json = conform_smash_case(source_json)
    cleaned_prop = {k: str(v or '') for (k, v) in  obj['properties'].items()}

    metadata = row_transform_and_convert(cleaned_json, cleaned_prop)

    return metadata


def scrape_csv_metadata(row, header, source):
    props = {}

    source_json = json.loads(open('{}/sources/{}'.format(config.openaddr_dir, source)).read())
    cleaned_json = conform_smash_case(source_json)
    for key in header:
        if key != 'OA:geom':
            props[key] = row[header.index(key)]

    cleaned_prop = {k: str(v or '') for (k, v) in  props.items()}
    metadata = row_transform_and_convert(cleaned_json, cleaned_prop)

    return metadata


def import_with_fiona(fpath, source):
    """Return a list of shapely geometries.

    Given a filepath, import data using fiona and cast to shapely.
    """
    shapes = []

    try:
        with fiona.drivers():
            data = fiona.open(fpath)
            for obj in data:
                try:
                    shape = scrape_fiona_metadata(obj, source)
                    geom = to_shapely_obj(obj)
                    if geom:
                        shape['geom'] = dumps(geom)
                        shapes.append(shape)
                except Exception as e:
                    print('  [-] error loading shape from fiona. {}'.format(e))
                    traceback.print_exc(file=sys.stdout)
    except Exception as e:
        print('  [-] error importing file. {}'.format(e))

    return shapes


def import_csv(fpath, source):
    """Import a csv document into shapely objects.

    Uses shapely to import a WKT file.
    """

    data = []
    try:
        csvdata = []
        with open(fpath, 'r') as f:
            statereader = csv.reader(f, delimiter=',')
            for row in statereader:
                csvdata.append(row)
        header = csvdata.pop(0)
        for row in csvdata:
            try:
                shape = scrape_csv_metadata(row, header, source)
                shape['geom'] = row[header.index('OA:geom')]
                data.append(shape)
            except Exception as e:
                print('  [-] error loading shape from csv. {}'.format(e))
    except Exception as e:
        print('  [-] error importing csv. {}'.format(e))

    return data
