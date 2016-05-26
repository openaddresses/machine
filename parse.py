import csv
import traceback
import os
import re
import shutil
import sys

from shapely.wkt import dumps
from utils import fetch, unzip, rlistdir, import_with_fiona, import_csv

fiona_extensions = ['shp', 'geojson']
csv.field_size_limit(sys.maxsize)


def parse_source(source, idx, header):
    """Import data from a single source in state.txt

    source: a line of imported csv
    idx: an index that's used to create a folder on disk for
      temporarily working with the given data.
    header: the first line of csv, so that we know which columns
      contain the data we want to parse.
    """
    path = './workspace/{}'.format(idx)
    if not os.path.exists(path):
        os.makedirs(path)

    cache_url = source[header.index('cache')]
    cache_filename = re.search('/[^/]*$', cache_url).group()
    fetch(cache_url, path + cache_filename)

    files = rlistdir(path)
    for f in files:
        if re.match('.*\.(zip|obj|exe)$', f):  # some files had mislabelled ext
            unzip(f, path)

    shapes = []
    files = rlistdir(path)
    for f in files:
        if re.match('.*\.({})$'.format('|'.join(fiona_extensions)), f):
            objs = import_with_fiona(f, source[0])
            for obj in objs:
                shapes.append(obj)
        elif re.match('.*\.csv$', f):
            objs = import_csv(f, source[0])
            for obj in objs:
                shapes.append(obj)

    shutil.rmtree(path)

    if not shapes:
        print('  [-] did not find shapes. files in archive: {}'.format(files))
    return shapes


def writeout(fp, data):
    keys = list(data[0].keys())

    writer = csv.DictWriter(fp, fieldnames=keys)
    writer.writeheader()
    for row in data:
        writer.writerow(row)

    fp.close()


def parse_statefile(state, header):
    """Imports all available data from state.txt

    Note: We catch all errors during processing, give a warning,
    and churn on, this is the preferred data processing method.
    """
    ct = 0
    for idx in range(0, len(state)):
        try:
            data = parse_source(state[idx], idx, header)
            if data:
                ct += 1
                wkt_file = open("./output/{}.wkt".format(idx), 'w')
                writeout(wkt_file, data)
        except Exception as e:
            print('  [-] error parsing source. {}'.format(e))
            traceback.print_exc(file=sys.stdout)
        print('parsed {} [{}/{}]'.format(idx + 1, ct, len(state)))


def load_state():
    """Loads a python representation of the state file.

    Looks in the current working directory, and returns both
    the state, and the column header as a different object.
    """
    state = []
    with open('state.txt', 'r') as statefile:
        statereader = csv.reader(statefile, delimiter='	')
        for row in statereader:
            state.append(row)

    header = state.pop(0)
    return state, header


def filter_polygons(state, header):
    """Removes any non-polygon sources from the state file.

    We are only interested in parsing parcel data, which is
    marked as Polygon in the state file.
    """
    filtered_state = []

    for source in state:
        if 'Polygon' in source[header.index('geometry type')]:
            filtered_state.append(source)

    return filtered_state


if __name__ == '__main__':
    """Parse parcel data from the latest state file.

    Download state.txt if it doesn't exist, and dump all
    csv data into ./output
    """
    if not os.path.isfile('./state.txt'):
        print('[+] fetching state.txt')
        fetch('http://results.openaddresses.io/state.txt', './state.txt')

    path = './output'
    if not os.path.exists(path):
        os.makedirs(path)

    raw_state, header = load_state()
    state = filter_polygons(raw_state, header)

    parse_statefile(state, header)
