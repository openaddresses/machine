import logging; _L = logging.getLogger('openaddr.ci.coverage.calculate')

import sys, json, os, tempfile
from argparse import ArgumentParser
from urllib.parse import urljoin

import requests
import psycopg2
from osgeo import ogr

from .. import setup_logger

state_codes = {
    '01': 'AL', '31': 'NE', '02': 'AK', '32': 'NV', '04': 'AZ', '33': 'NH',
    '05': 'AR', '34': 'NJ', '06': 'CA', '35': 'NM', '08': 'CO', '36': 'NY',
    '09': 'CT', '37': 'NC', '10': 'DE', '38': 'ND', '11': 'DC', '39': 'OH',
    '12': 'FL', '40': 'OK', '13': 'GA', '41': 'OR', '15': 'HI', '42': 'PA',
    '16': 'ID', '72': 'PR', '17': 'IL', '44': 'RI', '18': 'IN', '45': 'SC',
    '19': 'IA', '46': 'SD', '20': 'KS', '47': 'TN', '21': 'KY', '48': 'TX',
    '22': 'LA', '49': 'UT', '23': 'ME', '50': 'VT', '24': 'MD', '51': 'VA',
    '25': 'MA', '78': 'VI', '26': 'MI', '53': 'WA', '27': 'MN', '54': 'WV',
    '28': 'MS', '55': 'WI', '29': 'MO', '56': 'WY', '30': 'MT'
    } 	

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

def guess_state_abbrev(feature):
    '''
    '''
    state_abbrev = None

    if feature.GetField('US Census GEOID'):
        # Assume US based on Census GEOID
        state_fips = feature.GetField('US Census GEOID')[:2].upper()
        
        if state_fips in state_codes:
            state_abbrev = state_codes[state_fips]

    elif feature.GetField('source paths'):
        # Read from paths, like "sources/xx/place.json"
        paths = feature.GetField('source paths')
        try:
            _, iso_a2, state_abbrev, _ = paths.upper().split(os.path.sep, 3)
        except ValueError:
            pass
        else:
            if iso_a2 != 'US':
                state_abbrev = None

    return state_abbrev

def insert_coverage_feature(db, feature):
    ''' Add a feature of coverage to temporary rendered_world table.
    '''
    geom = validate_geometry(feature.GetGeometryRef())
    iso_a2, state_abbrev = guess_iso_a2(feature), guess_state_abbrev(feature)

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
    
    if iso_a2 == 'US' and state_abbrev:
        db.execute('''INSERT INTO rendered_usa (usps_code, count, geom)
                      VALUES(%s, %s, ST_Multi(ST_SetSRID(%s::geometry, 4326)))''',
                   (state_abbrev, feature.GetField('address count'), geom_wkt))
    
    return iso_a2, state_abbrev

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

def summarize_us_state_coverage(db, usps_code):
    ''' Populate area and population columns in areas table from gpwv4_2015_us table.
    '''
    db.execute('''
        WITH
            --
            -- 1x1 boxes of Natural Earth coverage, with GPWv4 population and area.
            --
            cb_boxes AS (
            SELECT box.id, cb.name, gpw.area, gpw.population,
                ST_Intersection(cb.geom, box.geom) AS geom
            FROM cb_2013_us_state_20m as cb, gpwv4_2015_us as gpw, boxes as box
            WHERE cb.usps_code = %s
              AND cb.usps_code = gpw.usps_code
              AND gpw.box_id = box.id
              AND box.size = 1.0
            ),
            --
            -- 1x1 boxes of OpenAddresses coverage, shapes only.
            --
            oa_boxes AS (
            SELECT box.id, ST_Intersection(oa.geom, box.geom) AS geom
            FROM areas_us as oa, gpwv4_2015_us as gpw, boxes as box
            WHERE oa.usps_code = %s
              AND oa.usps_code = gpw.usps_code
              AND gpw.box_id = box.id
              AND box.size = 1.0
            )

        SELECT
            --
            -- Compare OA area coverage with census area coverage for
            -- each 1x1 degree box to estimate area and population.
            --
            SUM(cb_boxes.area * ST_Area(ST_Intersection(oa_boxes.geom, cb_boxes.geom)) / ST_Area(cb_boxes.geom)) AS area_total,
            SUM(cb_boxes.population * ST_Area(ST_Intersection(oa_boxes.geom, cb_boxes.geom)) / ST_Area(cb_boxes.geom)) AS population_total,
            SUM(cb_boxes.area * ST_Area(ST_Intersection(oa_boxes.geom, cb_boxes.geom)) / ST_Area(cb_boxes.geom)) / SUM(cb_boxes.area) AS area_pct,
            SUM(cb_boxes.population * ST_Area(ST_Intersection(oa_boxes.geom, cb_boxes.geom)) / ST_Area(cb_boxes.geom)) / SUM(cb_boxes.population) AS population_pct,
            MIN(cb_boxes.name) AS name

        FROM cb_boxes LEFT JOIN oa_boxes
        ON cb_boxes.id = oa_boxes.id
        WHERE ST_Area(cb_boxes.geom) > 0;
        ''',
        (usps_code, usps_code))
    
    (area_total, pop_total, area_pct, pop_pct, name) = db.fetchone()
    
    db.execute('''UPDATE areas_us SET name = %s, area_total = %s, area_pct = %s,
                  pop_total = %s, pop_pct = %s WHERE usps_code = %s''',
               (name, area_total, area_pct, pop_total, pop_pct, usps_code))

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
    setup_logger(args.sns_arn, None, log_level=args.loglevel)
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
            iso_a2s, usps_codes = set(), set()
            rendered_ds = ogr.Open(filename)

            db.execute('''
                CREATE TEMPORARY TABLE rendered_world
                (
                    iso_a2  VARCHAR(2),
                    count   INTEGER,
                    geom    GEOMETRY(MultiPolygon, 4326)
                );

                CREATE TEMPORARY TABLE rendered_usa
                (
                    usps_code   VARCHAR(2),
                    count       INTEGER,
                    geom        GEOMETRY(MultiPolygon, 4326)
                );
                ''')

            for feature in rendered_ds.GetLayer(0):
                iso_a2, usps_code = insert_coverage_feature(db, feature)
                iso_a2s.add(iso_a2)
                
                if usps_code:
                    usps_codes.add(usps_code)
                    print('{}/{} - {} addresses from {}'.format(iso_a2, usps_code, feature.GetField('address count'), feature.GetField('source paths')))
                else:
                    _L.debug('{} - {} addresses from {}'.format(iso_a2, feature.GetField('address count'), feature.GetField('source paths')))
        
            db.execute('''
                DELETE FROM areas;
            
                INSERT INTO areas (iso_a2, addr_count, buffer_km, geom)
                SELECT iso_a2, SUM(count), 10, ST_Multi(ST_Union(ST_Buffer(geom, 0.00001)))
                FROM rendered_world GROUP BY iso_a2;

                DELETE FROM areas_us;
            
                INSERT INTO areas_us (usps_code, addr_count, buffer_km, geom)
                SELECT usps_code, SUM(count), 10, ST_Multi(ST_Union(ST_Buffer(geom, 0.00001)))
                FROM rendered_usa GROUP BY usps_code;
                ''')
        
            for (index, iso_a2) in enumerate(sorted(iso_a2s)):
                _L.info('Counting up {} ({}/{})...'.format(iso_a2, index+1, len(iso_a2s)))
                summarize_country_coverage(db, iso_a2)

            for (index, usps_code) in enumerate(sorted(usps_codes)):
                print('Counting up US:{} ({}/{})...'.format(usps_code, index+1, len(usps_codes)))
                summarize_us_state_coverage(db, usps_code)

    os.remove(filename)
