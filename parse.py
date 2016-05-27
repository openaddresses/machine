import csv
import logging
import os
import re
import shutil
import sys
import traceback

import config

from openaddr.jobs import setup_logger
from shapely.wkt import dumps
from utils import fetch, unzip, rlistdir, import_with_fiona, import_csv

_L = logging.getLogger('openaddr.parcels')

csv.field_size_limit(sys.maxsize)
setup_logger()

def parse_source(source, idx, header):
    """
    Import data from a single source based on the data type.
    """
    path = '{}/{}'.format(config.workspace_dir, idx)
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
        if re.match('.*\.({})$'.format('|'.join(config.fiona_extensions)), f):
            objs = import_with_fiona(f, source[0])
            for obj in objs:
                shapes.append(obj)
        elif re.match('.*\.csv$', f):
            objs = import_csv(f, source[0])
            for obj in objs:
                shapes.append(obj)

    shutil.rmtree(path)

    if not shapes:
        _L.warning('failed to parse source. did not find shapes. files in archive: {}'.format(files))

    return shapes


def writeout(fp, data):
    """
    Write the csv file.
    """
    keys = list(data[0].keys())

    writer = csv.DictWriter(fp, fieldnames=keys)
    writer.writeheader()
    for row in data:
        writer.writerow(row)

    fp.close()


def parse_statefile(state, header):
    """
    Imports all available data from state.

    Note: We catch all errors during processing, give a warning,
    and churn on, this is the preferred data processing method.
    """
    ct = 0
    for idx in range(0, len(state)):
        try:
            data = parse_source(state[idx], idx, header)
            if data:
                filename = re.sub(r'\.[^\.]*$', '.csv', state[idx][header.index('source')])
                os.makedirs('{}/{}'.format(config.output_dir, re.sub(r'\/[^\/]*$', '', filename)))
                wkt_file = open("{}/{}".format(config.output_dir, filename), 'w')
                writeout(wkt_file, data)
                ct += 1
        except Exception as e:
            _L.warning('error parsing source. {}'.format(e))

        _L.info('parsed {} [{}/{}]'.format(idx + 1, ct, len(state)))


def load_state():
    """
    Loads a python representation of the state file.
    """
    state = []
    with open(config.statefile_path, 'r') as statefile:
        statereader = csv.reader(statefile, dialect='excel-tab')
        for row in statereader:
            state.append(row)

    header = state.pop(0)
    return state, header


def filter_polygons(state, header):
    """
    Removes any non-polygon sources from the state file.

    We are only interested in parsing parcel data, which is
    marked as Polygon in the state file.
    """
    filtered_state = []

    for source in state:
        if 'Polygon' in source[header.index('geometry type')]:
            filtered_state.append(source)

    return filtered_state


if __name__ == '__main__':
    if not os.path.isfile(config.statefile_path):
        fetch(config.state_url, config.statefile_path)

    if not os.path.exists(config.output_dir):
        os.makedirs(config.output_dir)

    raw_state, header = load_state()
    state = filter_polygons(raw_state, header)

    parse_statefile(state, header)
