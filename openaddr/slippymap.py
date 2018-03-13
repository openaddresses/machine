from __future__ import division
import logging; _L = logging.getLogger('openaddr.slippymap')

from zipfile import ZipFile
from io import TextIOWrapper
from csv import DictReader
from tempfile import gettempdir, mkstemp
from argparse import ArgumentParser
from urllib.parse import urlparse
import os, subprocess, json
import requests

def generate(mbtiles_filename, *filenames_or_urls):
    '''
    '''
    cmd = 'tippecanoe', '-l', 'dots', '-r', '3', \
          '-n', 'OpenAddresses Dots', '-f', \
          '-t', gettempdir(), '-o', mbtiles_filename

    tippecanoe = subprocess.Popen(cmd, stdin=subprocess.PIPE, bufsize=1)

    for filename_or_url in filenames_or_urls:
        src_filename = get_local_filename(filename_or_url)

        for feature in iterate_file_features(src_filename):
            tippecanoe.stdin.write(json.dumps(feature).encode('utf8'))
            tippecanoe.stdin.write(b'\n')

    tippecanoe.stdin.close()
    tippecanoe.wait()

def get_local_filename(filename_or_url):
    '''
    '''
    parsed = urlparse(filename_or_url)
    suffix = os.path.splitext(parsed.path)[1]

    if parsed.scheme in ('', 'file'):
        return filename_or_url

    if parsed.scheme not in ('http', 'https'):
        raise ValueError('Unknown URL type: {}'.format(filename_or_url))

    _L.info('Downloading {}...'.format(filename_or_url))

    got = requests.get(filename_or_url)
    _, filename = mkstemp(prefix='SlippyMap-', suffix=suffix)

    with open(filename, 'wb') as file:
        file.write(got.content)
        _L.debug('Saved to {}'.format(filename))

    return filename

def iterate_file_features(filename):
    ''' Stream GeoJSON features from an input .csv or .zip file.
    '''
    suffix = os.path.splitext(filename)[1].lower()

    if suffix == '.csv':
        open_file = open(filename, 'r')
    elif suffix == '.zip':
        open_file = open(filename, 'rb')

    with open_file as file:
        if suffix == '.csv':
            csv_file = file
        elif suffix == '.zip':
            zip = ZipFile(file)
            csv_names = [name for name in zip.namelist() if name.endswith('.csv')]
            csv_file = TextIOWrapper(zip.open(csv_names[0]))

        for row in DictReader(csv_file):
            try:
                lon, lat = float(row['LON']), float(row['LAT'])
            except:
                continue

            if -180 <= lon <= 180 and -90 <= lat <= 90:
                geometry = dict(type='Point', coordinates=[lon, lat])
                properties = {k: v for (k, v) in row.items() if k not in ('LON', 'LAT')}
                feature = dict(type='Feature', geometry=geometry, properties=properties)
                yield(feature)

parser = ArgumentParser(description='Generate a single source slippy map MBTiles file with Tippecanoe.')

parser.add_argument('mbtiles_filename', help='Output MBTiles filename.')
parser.add_argument('src_filenames', help='Input Zip or CSV filename or URL.', nargs='*')

parser.add_argument('-v', '--verbose', help='Turn on verbose logging',
                    action='store_const', dest='loglevel',
                    const=logging.DEBUG, default=logging.INFO)

parser.add_argument('-q', '--quiet', help='Turn off most logging',
                    action='store_const', dest='loglevel',
                    const=logging.WARNING, default=logging.INFO)

def main():
    args = parser.parse_args()
    from .ci import setup_logger
    setup_logger(None, None, log_level=args.loglevel)
    generate(args.mbtiles_filename, *args.src_filenames)

if __name__ == '__main__':
    exit(main())
