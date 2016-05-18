import csv
import fiona
import os
import sys
import re
import shutil

from shapely.geometry import shape, MultiPolygon
from shapely.wkt import dumps
from utils import fetch, unzip

fiona_extensions = ['shp', 'geojson']


def to_shapely_obj(data):
    try:
        geom = shape(data['geometry'])
        if not geom.is_valid:
            clean = geom.buffer(0.0)
            assert clean.is_valid
            assert clean.geom_type == 'Polygon'
            geom = clean

        if geom.geom_type == 'Polygon':
            return geom

    except Exception as e:
        pass
        print('[-] error converting shape. {}'.format(e))

    return None


def import_with_fiona(fpath):
    try:
        with fiona.drivers():
            shapes = []

            dat = fiona.open(fpath)
            for s in dat:
                x = to_shapely_obj(s)
                if x:
                    shapes.append(x)

            if len(shapes):
                return shapes

    except Exception as e:
        pass
        print('[-] error importing file. {}'.format(e))

    return []


def import_csv(fpath):
    try:
        with open(fpath, 'r') as f:
            if f.read():
                print("[+] read csv")
    except Exception as e:
        print('[-] error importing csv. {}'.format(e))


def parse_source(source, idx, header):
    path = './workspace/{}'.format(idx)
    if not os.path.exists(path):
        os.makedirs(path)

    cache_url = source[header.index('cache')]
    cache_filename = re.search('/[^/]*$', cache_url).group()
    fetch(cache_url, path + cache_filename)

    files = os.listdir(path)
    for f in files:
        if re.match('.*\.zip$', f):
            unzip(path + '/' + f, path)

    shapes = []
    files = os.listdir(path)
    print(files)
    for f in files:
        if re.match('.*\.({})$'.format('|'.join(fiona_extensions)), f):
            print('[+] found valid file. ' + f)
            objs = import_with_fiona(path + '/' + f)
            for obj in objs:
                shapes.append(obj)
        elif re.match('.*\.csv$', f):
            print('[+] found csv file')
            objs = import_csv(path + '/' + f)
            for obj in objs:
                shapes.append(obj)

    shutil.rmtree(path)

    if shapes:
        print('[+] shapes exist, assuming success.')
        return MultiPolygon(shapes)

    print('[-] did not find shapes. files in archive: {}'.format(files))
    return None


def parse_statefile(state, header):
    for idx in range(0, len(state)):
        sys.stdout.flush()
        try:
            data = parse_source(state[idx], idx, header)
            if data:
                wkt_file = open("./output/{}.wkt".format(idx), 'w')
                wkt_file.write(dumps(data))
                wkt_file.close()
        except Exception as e:
            pass
            print('[-] error parsing source. {}'.format(e))


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
