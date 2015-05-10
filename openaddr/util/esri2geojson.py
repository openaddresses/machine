''' Convenience utility for converting ESRI feature service to GeoJSON.
'''
from __future__ import absolute_import, division, print_function
import logging; _L = logging.getLogger('openaddr.util.esri2geojson')

from argparse import ArgumentParser
from os.path import dirname, join, basename, splitext, exists
from tempfile import mkdtemp
from csv import DictReader
from shutil import rmtree
from os import remove

from ..cache import EsriRestDownloadTask
from ..conform import GEOM_FIELDNAME

from osgeo import ogr

# index of numeric geometry types to names
geometry_types = dict([(getattr(ogr, attr), attr) for attr in dir(ogr)
                       if attr.startswith('wkb')])

def guess_geom_type(csv_path, geom_name):
    ''' Look at the first row of the given CSV to determine its geometry type.
    '''
    with open(csv_path) as file:
        for row in DictReader(file):
            geom = ogr.CreateGeometryFromWkt(row.get(geom_name))
            return geometry_types.get(geom.GetGeometryType(), False)
    
    return False

def write_vrt_file(csv_path):
    ''' Generate a VRT file to help OGR read CSV file.
    
        http://www.gdal.org/drv_vrt.html
    '''
    vrt_template = '''<OGRVRTDataSource>
            <OGRVRTLayer name="{csv_base}">
                <SrcDataSource>{csv_path}</SrcDataSource>
                <SrcLayer>{csv_base}</SrcLayer>
                <GeometryField encoding="WKT" name="vrt_geom" field="{geom_name}" reportSrcColumn="FALSE">
                    <GeometryType>{geom_type}</GeometryType>
                    <SRS>EPSG:4326</SRS>
                </GeometryField>
            </OGRVRTLayer>
        </OGRVRTDataSource>'''
    
    geom_name = GEOM_FIELDNAME
    geom_type = guess_geom_type(csv_path, geom_name)
    csv_dir = dirname(csv_path)
    csv_base, _ = splitext(basename(csv_path))
    vrt_path = join(csv_dir, csv_base + '.vrt')

    with open(vrt_path, 'w') as file:
        file.write(vrt_template.format(**locals()))

    _L.debug('Wrote {vrt_path}'.format(**locals()))
    
    return vrt_path

def esri2geojson(esri_url, geojson_path):
    ''' Convert single ESRI feature service URL to GeoJSON file.
    '''
    workdir = mkdtemp(prefix='esri2geojson-')
    ogr.UseExceptions()

    try:
        task = EsriRestDownloadTask('esri')
        (csv_path, ) = task.download([esri_url], workdir)

        _L.info('Saved {esri_url} to {csv_path}'.format(**locals()))
    
        vrt_path = write_vrt_file(csv_path)
    
        ds_in = ogr.Open(vrt_path)
        driver = ogr.GetDriverByName('GeoJSON')
    
        if exists(geojson_path):
            remove(geojson_path)
    
        ds_out = driver.CopyDataSource(ds_in, geojson_path)
        ds_out.Release()

        _L.info('Converted {csv_path} to {geojson_path}'.format(**locals()))
    
    finally:
        rmtree(workdir)

        _L.info('Removed {workdir}'.format(**locals()))

parser = ArgumentParser(description='Convert single ESRI feature service URL to GeoJSON file.')

parser.add_argument('esri_url', help='Required ESRI source URL.')
parser.add_argument('geojson_path', help='Required output GeoJSON filename.')

parser.add_argument('-l', '--logfile', help='Optional log file name.')

parser.add_argument('-v', '--verbose', help='Turn on verbose logging',
                    action='store_const', dest='loglevel',
                    const=logging.DEBUG, default=logging.INFO)

parser.add_argument('-q', '--quiet', help='Turn off most logging',
                    action='store_const', dest='loglevel',
                    const=logging.WARNING, default=logging.INFO)

def main():
    from ..jobs import setup_logger

    args = parser.parse_args()
    setup_logger(logfile=args.logfile, log_level=args.loglevel)

    return esri2geojson(args.esri_url, args.geojson_path)

if __name__ == '__main__':
    exit(main())
