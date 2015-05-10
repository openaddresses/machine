from __future__ import division
import logging; _L = logging.getLogger('openaddr.render')

from .compat import standard_library

from glob import glob
from argparse import ArgumentParser
from itertools import combinations
from os.path import join, dirname, basename
from urllib.parse import urljoin
import json

from .compat import cairo
from osgeo import ogr, osr
import requests

from . import paths

# Areas
WORLD, USA, EUROPE = 54029, 2163, 'Europe'

# WGS 84, http://spatialreference.org/ref/epsg/4326/
EPSG4326 = '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs'

# US National Atlas Equal Area, http://spatialreference.org/ref/epsg/2163/
EPSG2163 = '+proj=laea +lat_0=45 +lon_0=-100 +x_0=0 +y_0=0 +a=6370997 +b=6370997 +units=m +no_defs'

# World Van der Grinten I, http://spatialreference.org/ref/esri/54029/
ESRI54029 = '+proj=vandg +lon_0=0 +x_0=0 +y_0=0 +R_A +ellps=WGS84 +datum=WGS84 +units=m +no_defs'

def make_context(width=960, resolution=1, area=WORLD):
    ''' Get Cairo surface, context, and drawing scale.
    
        Global extent, World Van der Grinten I:
        (-19918964.35, -8269767.91) - (19918964.18, 14041770.96)
        http://spatialreference.org/ref/esri/54029/

        U.S. extent, National Atlas Equal Area:
        (-2031905.05, -2114924.96) - (2516373.83, 732103.34)
        http://spatialreference.org/ref/epsg/2163/
    
        Europe extent, World Van der Grinten I:
        (-2679330, 3644860) - (5725981, 8635513)
        http://spatialreference.org/ref/esri/54029/
    '''
    if area == WORLD:
        left, top = -18000000, 14050000
        right, bottom = 19500000, -7500000
    elif area == USA:
        left, top = -2040000, 740000
        right, bottom = 2525000, -2130000
    elif area == EUROPE:
        left, top = -2700000, 8700000
        right, bottom = 5800000, 3600000
    else:
        raise RuntimeError('Unknown area "{}"'.format(area))

    aspect = (right - left) / (top - bottom)

    hsize = int(resolution * width)
    vsize = int(hsize / aspect)

    hscale = hsize / (right - left)
    vscale = (hsize / aspect) / (bottom - top)

    hoffset = -left
    voffset = -top

    surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, hsize, vsize)
    context = cairo.Context(surface)
    context.scale(hscale, vscale)
    context.translate(hoffset, voffset)
    
    return surface, context, hscale

def load_live_state():
    '''
    '''
    got = requests.get('http://data.openaddresses.io/state.json')
    got = requests.get(urljoin(got.url, got.json()))

    columns, rows = got.json()[0], got.json()[1:]
    state = [dict(zip(columns, row)) for row in rows]

    good_sources = [s['source'] for s in state if (s['cache'] and s['processed'])]
    return set(good_sources)

def load_fake_state(sources_dir):
    '''
    '''
    fake_sources = set()

    for path in glob(join(sources_dir, '*.json')):
        fake_sources.add(basename(path))
    
    return fake_sources

def load_geoids(directory, good_sources):
    ''' Load a set of U.S. Census GEOIDs that should be rendered.
    '''
    good_geoids, bad_geoids = set(), set()

    for path in glob(join(directory, 'us-*.json')):
        with open(path) as file:
            data = json.load(file)
    
        if 'geoid' in data.get('coverage', {}).get('US Census', {}):
            if basename(path) in good_sources:
                good_geoids.add(data['coverage']['US Census']['geoid'])
            else:
                bad_geoids.add(data['coverage']['US Census']['geoid'])
    
    return good_geoids, bad_geoids

def load_iso3166s(directory, good_sources):
    ''' Load a set of ISO 3166 codes that should be rendered.
    '''
    good_iso3166s, bad_iso3166s = set(), set()

    for path in glob(join(directory, '*.json')):
        with open(path) as file:
            data = json.load(file)
    
        if 'code' in data.get('coverage', {}).get('ISO 3166', {}):
            if basename(path) in good_sources:
                good_iso3166s.add(data['coverage']['ISO 3166']['code'])
            else:
                bad_iso3166s.add(data['coverage']['ISO 3166']['code'])
    
        elif 'alpha2' in data.get('coverage', {}).get('ISO 3166', {}):
            if basename(path) in good_sources:
                good_iso3166s.add(data['coverage']['ISO 3166']['alpha2'])
            else:
                bad_iso3166s.add(data['coverage']['ISO 3166']['alpha2'])
    
    return good_iso3166s, bad_iso3166s

def load_geometries(directory, good_sources, area):
    ''' Load a set of GeoJSON geometries should be rendered.
    '''
    good_geometries, bad_geometries = list(), list()

    osr.UseExceptions()
    sref_geo = osr.SpatialReference(); sref_geo.ImportFromProj4(EPSG4326)
    sref_map = osr.SpatialReference(); sref_map.ImportFromProj4(EPSG2163 if area == USA else ESRI54029)
    project = osr.CoordinateTransformation(sref_geo, sref_map)

    for path in glob(join(directory, '*.json')):
        with open(path) as file:
            data = json.load(file)
    
        if 'geometry' in data.get('coverage', {}):
            geojson = json.dumps(data['coverage']['geometry'])
            geometry = ogr.CreateGeometryFromJson(geojson)
            
            if not geometry:
                continue

            geometry.Transform(project)

            if basename(path) in good_sources:
                good_geometries.append(geometry)
            else:
                bad_geometries.append(geometry)
    
    return good_geometries, bad_geometries

def stroke_features(ctx, features):
    '''
    '''
    return stroke_geometries(ctx, [f.GetGeometryRef() for f in features])
    
def stroke_geometries(ctx, geometries):
    '''
    '''
    for geometry in geometries:
        if geometry.GetGeometryType() in (ogr.wkbMultiPolygon, ogr.wkbMultiLineString):
            parts = geometry
        elif geometry.GetGeometryType() in (ogr.wkbPolygon, ogr.wkbLineString):
            parts = [geometry]
        else:
            continue

        for part in parts:
            if part.GetGeometryType() is ogr.wkbPolygon:
                rings = part
            else:
                rings = [part]

            for ring in rings:
                points = ring.GetPoints()
                if geometry.GetGeometryType() in (ogr.wkbPolygon, ogr.wkbMultiPolygon):
                    draw_line(ctx, points[-1], points)
                else:
                    draw_line(ctx, points[0], points[1:])
                ctx.stroke()

def fill_features(ctx, features, muppx, rgb):
    '''
    '''
    return fill_geometries(ctx, [f.GetGeometryRef() for f in features], muppx, rgb)
    
def fill_geometries(ctx, geometries, muppx, rgb):
    '''
    '''
    ctx.set_source_rgb(*rgb)

    for geometry in geometries:
        if geometry.GetGeometryType() == ogr.wkbMultiPolygon:
            parts = geometry
        elif geometry.GetGeometryType() == ogr.wkbPolygon:
            parts = [geometry]
        elif geometry.GetGeometryType() == ogr.wkbPoint:
            buffer = geometry.Buffer(2 * muppx, 3)
            parts = [buffer]
        else:
            raise NotImplementedError()

        for part in parts:
            for ring in part:
                points = ring.GetPoints()
                draw_line(ctx, points[-1], points)
            ctx.fill()

def draw_line(ctx, start, points):
    '''
    '''
    ctx.move_to(*start)

    for point in points:
        ctx.line_to(*point)

parser = ArgumentParser(description='Draw a map of worldwide address coverage.')

parser.set_defaults(resolution=1, width=960, area=WORLD)

parser.add_argument('--2x', dest='resolution', action='store_const', const=2,
                    help='Draw at double resolution.')

parser.add_argument('--1x', dest='resolution', action='store_const', const=1,
                    help='Draw at normal resolution.')

parser.add_argument('--width', dest='width', type=int,
                    help='Width in pixels.')

parser.add_argument('--use-state', dest='use_state', action='store_const',
                    const=True, default=False, help='Use live state from http://data.openaddresses.io/state.json.')

parser.add_argument('filename', help='Output PNG filename.')

parser.add_argument('--world', dest='area', action='store_const', const=WORLD,
                    help='Render the whole world.')

parser.add_argument('--usa', dest='area', action='store_const', const=USA,
                    help='Render the United States.')

parser.add_argument('--europe', dest='area', action='store_const', const=EUROPE,
                    help='Render Europe.')

def main():
    args = parser.parse_args()
    good_sources = load_live_state() if args.use_state else load_fake_state(paths.sources)
    return render(paths.sources, good_sources, args.width, args.resolution,
                  args.filename, args.area)

def render(sources_dir, good_sources, width, resolution, filename=None, area=WORLD):
    ''' Resolution: 1 for 100%, 2 for 200%, etc.
    '''
    if filename is None:
        _L.warning('Using deprecated arguments for openaddr.render.render()')
    
        # Adapt to old arguments: (sources_dir, width, resolution, filename)
        width, resolution, filename = good_sources, width, resolution
    
        # Use fake sources
        good_sources = load_fake_state(sources_dir)
    
    return _render_state(sources_dir, good_sources, width, resolution, filename, area)

def first_layer_list(datasource):
    ''' Return features as a list, or an empty list.
    '''
    return list(datasource.GetLayer(0) if hasattr(datasource, 'GetLayer') else [])

def _render_state(sources_dir, good_sources, width, resolution, filename, area):
    ''' Resolution: 1 for 100%, 2 for 200%, etc.
    '''
    # Prepare output surface
    surface, context, scale = make_context(width, resolution, area)
    
    # Load data
    good_geoids, bad_geoids = load_geoids(sources_dir, good_sources)
    good_iso3166s, bad_iso3166s = load_iso3166s(sources_dir, good_sources)
    good_geometries, bad_geometries = load_geometries(sources_dir, good_sources, area)

    geodata = join(dirname(__file__), 'geodata')

    if area in (WORLD, EUROPE):
        landarea_ds = ogr.Open(join(geodata, 'ne_50m_admin_0_countries-54029.shp'))
        coastline_ds = ogr.Open(join(geodata, 'ne_50m_coastline-54029.shp'))
        lakes_ds = ogr.Open(join(geodata, 'ne_50m_lakes-54029.shp'))
        countries_ds = ogr.Open(join(geodata, 'ne_50m_admin_0_countries-54029.shp'))
        countries_borders_ds = ogr.Open(join(geodata, 'ne_50m_admin_0_boundary_lines_land-54029.shp'))
        admin1s_ds = ogr.Open(join(geodata, 'ne_10m_admin_1_states_provinces-54029.shp'))
        us_state_ds = ogr.Open(join(geodata, 'cb_2013_us_state_20m-54029.shp'))
        us_county_ds = ogr.Open(join(geodata, 'cb_2013_us_county_20m-54029.shp'))
    elif area == USA:
        landarea_ds = ogr.Open(join(geodata, 'cb_2013_us_nation_20m-2163.shp'))
        coastline_ds = ogr.Open(join(geodata, 'cb_2013_us_nation_20m-2163.shp'))
        lakes_ds = None
        countries_ds = None
        countries_borders_ds = None
        admin1s_ds = None
        us_state_ds = ogr.Open(join(geodata, 'cb_2013_us_state_20m-2163.shp'))
        us_county_ds = ogr.Open(join(geodata, 'cb_2013_us_county_20m-2163.shp'))
    else:
        raise RuntimeError('Unknown area "{}"'.format(area))

    # Pick out features
    landarea_features = first_layer_list(landarea_ds)
    coastline_features = first_layer_list(coastline_ds)
    lakes_features = [f for f in first_layer_list(lakes_ds) if f.GetField('scalerank') == 0]
    countries_features = first_layer_list(countries_ds)
    countries_borders_features = first_layer_list(countries_borders_ds)
    admin1s_features = first_layer_list(admin1s_ds)
    us_state_features = first_layer_list(us_state_ds)
    us_county_features = first_layer_list(us_county_ds)

    # Assign features to good or bad lists
    good_data_states = [f for f in us_state_features if f.GetFieldAsString('GEOID') in good_geoids]
    good_data_counties = [f for f in us_county_features if f.GetFieldAsString('GEOID') in good_geoids]
    bad_data_states = [f for f in us_state_features if f.GetFieldAsString('GEOID') in bad_geoids]
    bad_data_counties = [f for f in us_county_features if f.GetFieldAsString('GEOID') in bad_geoids]
    good_data_countries = [f for f in countries_features if f.GetFieldAsString('iso_a2') in good_iso3166s]
    good_data_admin1s = [f for f in admin1s_features if f.GetFieldAsString('iso_3166_2') in good_iso3166s]
    bad_data_countries = [f for f in countries_features if f.GetFieldAsString('iso_a2') in bad_iso3166s]
    bad_data_admin1s = [f for f in admin1s_features if f.GetFieldAsString('iso_3166_2') in bad_iso3166s]
    
    # Draw each border between neighboring states exactly once.
    state_borders = [s1.GetGeometryRef().Intersection(s2.GetGeometryRef())
                     for (s1, s2) in combinations(us_state_features, 2)
                     if s1.GetGeometryRef().Intersects(s2.GetGeometryRef())]
    
    # Set up some colors
    silver = 0xdd/0xff, 0xdd/0xff, 0xdd/0xff
    white = 0xff/0xff, 0xff/0xff, 0xff/0xff
    black = 0, 0, 0
    light_red = 244/0xff, 109/0xff, 67/0xff
    dark_red = 215/0xff, 48/0xff, 39/0xff
    light_green = 0x74/0xff, 0xA5/0xff, 0x78/0xff
    dark_green = 0x1C/0xff, 0x89/0xff, 0x3F/0xff
    
    # Map units per reference pixel (http://www.w3.org/TR/css3-values/#reference-pixel)
    muppx = resolution / scale
    
    # Fill land area background
    fill_features(context, landarea_features, muppx, silver)

    # Fill populated countries
    fill_features(context, bad_data_countries, muppx, light_red)
    fill_features(context, good_data_countries, muppx, light_green)

    # Fill Admin-1 (ISO-3166-2) subdivisions
    fill_features(context, bad_data_admin1s, muppx, light_red)
    fill_features(context, good_data_admin1s, muppx, light_green)

    # Fill populated U.S. states
    fill_features(context, bad_data_states, muppx, light_red)
    fill_features(context, good_data_states, muppx, light_green)

    # Fill populated U.S. counties
    fill_features(context, bad_data_counties, muppx, dark_red)
    fill_features(context, good_data_counties, muppx, dark_green)

    # Fill other given geometries
    fill_geometries(context, bad_geometries, muppx, dark_red)
    fill_geometries(context, good_geometries, muppx, dark_green)

    # Outline countries and boundaries, fill lakes
    context.set_source_rgb(*black)
    context.set_line_width(.25 * muppx)
    stroke_geometries(context, state_borders)
    stroke_features(context, countries_borders_features)

    fill_features(context, lakes_features, muppx, white)

    context.set_source_rgb(*black)
    context.set_line_width(.5 * muppx)
    stroke_features(context, coastline_features)

    # Output
    surface.write_to_png(filename)

if __name__ == '__main__':
    exit(main())
