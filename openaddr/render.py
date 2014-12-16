from __future__ import division

from glob import glob
from argparse import ArgumentParser
from itertools import combinations
from os.path import join, dirname
import json

from cairo import ImageSurface, Context, FORMAT_ARGB32
from osgeo import ogr, osr

from . import paths

def make_context(width=960, resolution=1):
    ''' Get Cairo surface, context, and drawing scale.
    
        World extent: (-19918964.35, -8269767.91) - (19918964.18, 14041770.96)
    '''
    left, top = -18000000, 14050000
    right, bottom = 19500000, -7500000
    aspect = (right - left) / (top - bottom)

    hsize = int(resolution * width)
    vsize = int(hsize / aspect)

    hscale = hsize / (right - left)
    vscale = (hsize / aspect) / (bottom - top)

    hoffset = -left
    voffset = -top

    surface = ImageSurface(FORMAT_ARGB32, hsize, vsize)
    context = Context(surface)
    context.scale(hscale, vscale)
    context.translate(hoffset, voffset)
    
    return surface, context, hscale

def load_geoids(directory):
    ''' Load a set of U.S. Census GEOIDs that should be rendered.
    '''
    geoids = set()

    for path in glob(join(directory, 'us-*.json')):
        with open(path) as file:
            data = json.load(file)
    
        if 'geoid' in data.get('coverage', {}).get('US Census', {}):
            geoids.add(data['coverage']['US Census']['geoid'])
    
    return geoids

def load_alpha2s(directory):
    ''' Load a set of ISO 3166 Alpha 2s that should be rendered.
    '''
    alpha2s = set()

    for path in glob(join(directory, '??.json')):
        with open(path) as file:
            data = json.load(file)
    
        if 'alpha2' in data.get('coverage', {}).get('ISO 3166', {}):
            alpha2s.add(data['coverage']['ISO 3166']['alpha2'])
    
    return alpha2s

def load_geometries(directory):
    ''' Load a set of GeoJSON geometries should be rendered.
    '''
    geometries = list()

    sref_geo = osr.SpatialReference(); sref_geo.ImportFromEPSG(4326)
    sref_map = osr.SpatialReference(); sref_map.ImportFromEPSG(54029)
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
            geometries.append(geometry)
    
    return geometries

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

def fill_features(ctx, features):
    '''
    '''
    return fill_geometries(ctx, [f.GetGeometryRef() for f in features])
    
def fill_geometries(ctx, geometries):
    '''
    '''
    for geometry in geometries:
        if geometry.GetGeometryType() == ogr.wkbMultiPolygon:
            parts = geometry
        elif geometry.GetGeometryType() == ogr.wkbPolygon:
            parts = [geometry]
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

parser = ArgumentParser(description='Draw a map of continental U.S. address coverage.')

parser.set_defaults(resolution=1, width=960)

parser.add_argument('--2x', dest='resolution', action='store_const', const=2,
                    help='Draw at double resolution.')

parser.add_argument('--1x', dest='resolution', action='store_const', const=1,
                    help='Draw at normal resolution.')

parser.add_argument('--width', dest='width', type=int,
                    help='Width in pixels.')

parser.add_argument('filename', help='Output PNG filename.')

def main():
    args = parser.parse_args()
    return render(paths.sources, args.width, args.resolution, args.filename)

def render(sources, width, resolution, filename):
    ''' Resolution: 1 for 100%, 2 for 200%, etc.
    '''
    # Prepare output surface
    surface, context, scale = make_context(width, resolution)

    # Load data
    geoids = load_geoids(sources)
    alpha2s = load_alpha2s(sources)
    geometries = load_geometries(sources)

    geodata = join(dirname(__file__), 'geodata')
    coastline_ds = ogr.Open(join(geodata, 'ne_50m_coastline-54029.shp'))
    lakes_ds = ogr.Open(join(geodata, 'ne_50m_lakes-54029.shp'))
    countries_ds = ogr.Open(join(geodata, 'ne_50m_admin_0_countries-54029.shp'))
    countries_borders_ds = ogr.Open(join(geodata, 'ne_50m_admin_0_boundary_lines_land-54029.shp'))
    us_state_ds = ogr.Open(join(geodata, 'cb_2013_us_state_20m-54029.shp'))
    us_county_ds = ogr.Open(join(geodata, 'cb_2013_us_county_20m-54029.shp'))

    coastline_features = list(coastline_ds.GetLayer(0))
    lakes_features = [f for f in lakes_ds.GetLayer(0) if f.GetField('scalerank') == 0]
    countries_features = list(countries_ds.GetLayer(0))
    countries_borders_features = list(countries_borders_ds.GetLayer(0))
    us_state_features = list(us_state_ds.GetLayer(0))
    us_county_features = list(us_county_ds.GetLayer(0))
    data_states = [f for f in us_state_features if f.GetFieldAsString('GEOID') in geoids]
    data_counties = [f for f in us_county_features if f.GetFieldAsString('GEOID') in geoids]
    data_countries = [f for f in countries_features if f.GetFieldAsString('iso_a2') in alpha2s]
    
    # Draw each border between neighboring states exactly once.
    state_borders = [s1.GetGeometryRef().Intersection(s2.GetGeometryRef())
                     for (s1, s2) in combinations(us_state_features, 2)
                     if s1.GetGeometryRef().Intersects(s2.GetGeometryRef())]
    
    # Fill countries background
    context.set_source_rgb(0xdd/0xff, 0xdd/0xff, 0xdd/0xff)
    fill_features(context, countries_features)

    # Fill populated countries
    context.set_source_rgb(0x74/0xff, 0xA5/0xff, 0x78/0xff)
    fill_features(context, data_countries)

    # Fill populated U.S. states
    context.set_source_rgb(0x74/0xff, 0xA5/0xff, 0x78/0xff)
    fill_features(context, data_states)

    # Fill populated U.S. counties
    context.set_source_rgb(0x1C/0xff, 0x89/0xff, 0x3F/0xff)
    fill_features(context, data_counties)

    # Fill other given geometries
    context.set_source_rgb(0x1C/0xff, 0x89/0xff, 0x3F/0xff)
    fill_geometries(context, geometries)

    # Outline countries and boundaries, fill lakes
    context.set_source_rgb(0, 0, 0)
    context.set_line_width(.25 * resolution / scale)
    stroke_geometries(context, state_borders)
    stroke_features(context, countries_borders_features)

    context.set_source_rgb(0xff/0xff, 0xff/0xff, 0xff/0xff)
    fill_features(context, lakes_features)

    context.set_source_rgb(0, 0, 0)
    context.set_line_width(.5 * resolution / scale)
    stroke_features(context, coastline_features)

    # Output
    surface.write_to_png(filename)

if __name__ == '__main__':
    exit(main())
