import csv
import fiona
import os
import requests
import sys
import zipfile

from shapely.geometry import shape
from shapely.wkt import loads

clean_geometries = False
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
    try:
        geom = shape(data['geometry'])
        if clean_geometries:
            if not geom.is_valid:  # sends warnings to stderr
                clean = geom.buffer(0.0)  # attempt to clean shape
                assert clean.is_valid
                geom = clean

        if geom.geom_type == 'Polygon':
            return geom

    except Exception as e:  # TODO: add stack trace here.
        print('  [-] error converting shape. {}'.format(e))

    return None


def import_with_fiona(fpath):
    """Return a list of shapely geometries.

    Given a filepath, import data using fiona and cast to shapely.
    """
    shapes = []

    try:
        with fiona.drivers():
            dat = fiona.open(fpath)
            for s in dat:
                x = to_shapely_obj(s)
                if x:
                    shapes.append(x)
    except Exception as e:
        print('  [-] error importing file. {}'.format(e))

    return shapes


def import_csv(fpath):
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
            raw_geom = row[header.index('OA:geom')]
            data.append(loads(raw_geom))
    except Exception as e:
        print('  [-] error importing csv. {}'.format(e))

    return data
