import logging; _L = logging.getLogger('openaddr.ci.coverage.calculate')

import sys, json, os, tempfile
from argparse import ArgumentParser
from urllib.parse import urljoin

import requests
import psycopg2
from osgeo import ogr

from .. import setup_logger

is_point = lambda geom: bool(geom.GetGeometryType() in (ogr.wkbPoint, ogr.wkbMultiPoint))
is_polygon = lambda geom: bool(geom.GetGeometryType() in (ogr.wkbPolygon, ogr.wkbMultiPolygon))

def validate_geometry(geometry):
    '''
    '''
    if is_point(geometry):
        # Points are easy, we love points
        return geometry

    elif is_polygon(geometry):
        # Polygons may be invalid
        if geometry.IsValid():
            return geometry
        else:
            # Buffer by a tiny amount to force validity
            return geometry.Buffer(0.00000001, 2)

    else:
        # Don't know what to do with other geometry types, so ignore them
        return None

def guess_iso_a2(feature):
    '''
    '''
    iso_a2 = None

    if feature.GetField('ISO 3166'):
        # Read directly from ISO 3166 field
        iso_a2 = feature.GetField('ISO 3166')

    elif feature.GetField('ISO 3166-2'):
        # Read from first half of dash-delimited ISO 3166-2 field
        iso_a2, _ = feature.GetField('ISO 3166-2').split('-', 2)

    elif feature.GetField('US Census GEOID'):
        # Assume US based on Census GEOID
        iso_a2 = 'US'

    elif feature.GetField('source paths'):
        # Read from paths, like "sources/xx/place.json"
        paths = feature.GetField('source paths')
        _, iso_a2, _ = paths.upper().split(os.path.sep, 2)

    return iso_a2

def insert_coverage_feature(db, feature):
    ''' Add a feature of coverage to temporary rendered_world table.
    '''
    geom = validate_geometry(feature.GetGeometryRef())
    iso_a2 = guess_iso_a2(feature)

    if not geom:
        return
    
    if is_point(geom):
        # Ask PostGIS to buffer points by 10km, as a reasonable city size
        db.execute('SELECT ST_AsText(ST_Buffer(%s::geography, 10000))', (geom.ExportToWkt(), ))
        (geom_wkt, ) = db.fetchone()
    else:
        geom_wkt = geom.ExportToWkt()
    
    db.execute('''INSERT INTO rendered_world (iso_a2, count, geom)
                  VALUES(%s, %s, ST_Multi(ST_SetSRID(%s::geometry, 4326)))''',
               (iso_a2, feature.GetField('address count'), geom_wkt))
    
    return iso_a2

def summarize_country_coverage(db, iso_a2):
    ''' Populate area and population columns in areas table from gpwv4_2015 table.
    '''
    db.execute('''
        WITH
            --
            -- 1x1 boxes of Natural Earth coverage, with GPWv4 population and area.
            --
            ne_boxes AS (
            SELECT box.id, ne.name, gpw.area, gpw.population,
                ST_Intersection(ne.geom, box.geom) AS geom
            FROM ne_50m_admin_0_countries as ne, gpwv4_2015 as gpw, boxes as box
            WHERE ne.iso_a2 = %s
              AND ne.iso_a2 = gpw.iso_a2
              AND gpw.box_id = box.id
              AND box.size = 1.0
            ),
            --
            -- 1x1 boxes of OpenAddresses coverage, shapes only.
            --
            oa_boxes AS (
            SELECT box.id, ST_Intersection(oa.geom, box.geom) AS geom
            FROM areas as oa, gpwv4_2015 as gpw, boxes as box
            WHERE oa.iso_a2 = %s
              AND oa.iso_a2 = gpw.iso_a2
              AND gpw.box_id = box.id
              AND box.size = 1.0
            )

        SELECT
            --
            -- Compare OA area coverage with NE area coverage for
            -- each 1x1 degree box to estimate area and population.
            --
            SUM(ne_boxes.area * ST_Area(ST_Intersection(oa_boxes.geom, ne_boxes.geom)) / ST_Area(ne_boxes.geom)) AS area_total,
            SUM(ne_boxes.population * ST_Area(ST_Intersection(oa_boxes.geom, ne_boxes.geom)) / ST_Area(ne_boxes.geom)) AS population_total,
            SUM(ne_boxes.area * ST_Area(ST_Intersection(oa_boxes.geom, ne_boxes.geom)) / ST_Area(ne_boxes.geom)) / SUM(ne_boxes.area) AS area_pct,
            SUM(ne_boxes.population * ST_Area(ST_Intersection(oa_boxes.geom, ne_boxes.geom)) / ST_Area(ne_boxes.geom)) / SUM(ne_boxes.population) AS population_pct,
            MIN(ne_boxes.name) AS name

        FROM ne_boxes LEFT JOIN oa_boxes
        ON ne_boxes.id = oa_boxes.id
        WHERE ST_Area(ne_boxes.geom) > 0;
        ''',
        (iso_a2, iso_a2))
    
    (area_total, pop_total, area_pct, pop_pct, name) = db.fetchone()
    
    db.execute('''UPDATE areas SET name = %s, area_total = %s, area_pct = %s,
                  pop_total = %s, pop_pct = %s WHERE iso_a2 = %s''',
               (name, area_total, area_pct, pop_total, pop_pct, iso_a2))

START_URL = 'https://results.openaddresses.io/index.json'

parser = ArgumentParser(description='Calculate current worldwide address coverage.')

parser.add_argument('-d', '--database-url', default=os.environ.get('DATABASE_URL', None),
                    help='Optional connection string for database. Defaults to value of DATABASE_URL environment variable.')

parser.add_argument('--sns-arn', default=os.environ.get('AWS_SNS_ARN', None),
                    help='Optional AWS Simple Notification Service (SNS) resource. Defaults to value of AWS_SNS_ARN environment variable.')

parser.add_argument('-v', '--verbose', help='Turn on verbose logging',
                    action='store_const', dest='loglevel',
                    const=logging.DEBUG, default=logging.INFO)

parser.add_argument('-q', '--quiet', help='Turn off most logging',
                    action='store_const', dest='loglevel',
                    const=logging.WARNING, default=logging.INFO)

def main():
    '''
    '''
    args = parser.parse_args()
    setup_logger(None, None, args.sns_arn, log_level=args.loglevel)
    calculate(args.database_url)

def calculate(DATABASE_URL):
    '''
    '''
    index = requests.get(START_URL).json()
    geojson_url = urljoin(START_URL, index['render_geojson_url'])
    _L.info('Downloading {}...'.format(geojson_url))

    handle, filename = tempfile.mkstemp(prefix='render_geojson-', suffix='.geojson')
    geojson = os.write(handle, requests.get(geojson_url).content)
    os.close(handle)

    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as db:

            ogr.UseExceptions()
            iso_a2s = set()
            rendered_ds = ogr.Open(filename)

            db.execute('''
                CREATE TEMPORARY TABLE rendered_world
                (
                    iso_a2  VARCHAR(2),
                    count   INTEGER,
                    geom    GEOMETRY(MultiPolygon, 4326)
                );
                ''')

            for feature in rendered_ds.GetLayer(0):
                iso_a2 = insert_coverage_feature(db, feature)
                iso_a2s.add(iso_a2)
                _L.debug('{} - {} addresses from {}'.format(iso_a2, feature.GetField('address count'), feature.GetField('source paths')))
        
            db.execute('''
                DELETE FROM areas;
            
                INSERT INTO areas (iso_a2, addr_count, buffer_km, geom)
                SELECT iso_a2, SUM(count), 10, ST_Multi(ST_Union(ST_Buffer(geom, 0.00001)))
                FROM rendered_world GROUP BY iso_a2;
                ''')
        
            for (index, iso_a2) in enumerate(sorted(iso_a2s)):
                _L.info('Counting up {} ({}/{})...'.format(iso_a2, index+1, len(iso_a2s)))
                summarize_country_coverage(db, iso_a2)

    os.remove(filename)
