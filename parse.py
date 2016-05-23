import csv
import fiona
import os
import sys
import re
import shutil

from shapely.geometry import shape, MultiPolygon
from shapely.wkt import dumps, loads
from utils import fetch, unzip, rlistdir

csv.field_size_limit(sys.maxsize)
fiona_extensions = ['shp', 'geojson']


def to_shapely_obj(data):
    try:
        geom = shape(data['geometry'])
        if not geom.is_valid:  # sends warnings to stderr
            clean = geom.buffer(0.0)
            assert clean.is_valid
            assert clean.geom_type == 'Polygon'
            geom = clean

        if geom.geom_type == 'Polygon':
            return geom

    except Exception as e:
        print('  [-] error converting shape. {}'.format(e))

    return None


def import_with_fiona(fpath):
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


def parse_source(source, idx, header):
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
            objs = import_with_fiona(f)
            for obj in objs:
                shapes.append(obj)
        elif re.match('.*\.csv$', f):
            objs = import_csv(f)
            for obj in objs:
                shapes.append(obj)

    shutil.rmtree(path)

    if shapes:
        print('  [+] shapes exist, assuming success.')
        return MultiPolygon(shapes)

    print('  [-] did not find shapes. files in archive: {}'.format(files))
    return None


def parse_statefile(state, header):
    ct = 0
    for idx in range(0, len(state)):
        print('[+] parsing {} [{}/{}]'.format(idx, ct, len(state)))
        try:
            data = parse_source(state[idx], idx, header)
            if data:
                ct += 1
                wkt_file = open("./output/{}.wkt".format(idx), 'w')
                wkt_file.write(dumps(data))
                wkt_file.close()
        except Exception as e:
            print('  [-] error parsing source. {}'.format(e))


def load_state():
    state = []
    with open('state.txt', 'r') as statefile:
        statereader = csv.reader(statefile, delimiter='	')
        for row in statereader:
            state.append(row)

    header = state.pop(0)
    return state, header


def filter_polygons(state, header):
    filtered_state = []

    for source in state:
        if 'Polygon' in source[header.index('geometry type')]:
            filtered_state.append(source)

    return filtered_state


if __name__ == '__main__':
    if not os.path.isfile('./state.txt'):
        print('[+] fetching state.txt')
        fetch('http://results.openaddresses.io/state.txt', './state.txt')

    path = './output'
    if not os.path.exists(path):
        os.makedirs(path)

    raw_state, header = load_state()
    state = filter_polygons(raw_state, header)

    parse_statefile(state, header)
