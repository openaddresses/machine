from __future__ import division

from glob import glob
from argparse import ArgumentParser
from itertools import combinations
from os.path import join, dirname
import json

from cairo import ImageSurface, Context, FORMAT_ARGB32
from osgeo import ogr

from . import paths

def make_context(width=960, resolution=1):
    ''' Get Cairo surface, context, and drawing scale.
    
        U.S. extent: (-2031905.05, -2114924.96) - (2516373.83, 732103.34)
    '''
    left, top = -2040000, 740000
    right, bottom = 2525000, -2130000
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
    ''' Load a set of GEOIDs that should be rendered.
    '''
    geoids = set()

    for path in glob(join(directory, 'us-*.json')):
        with open(path) as file:
            data = json.load(file)
    
        if 'geoid' in data.get('coverage', {}).get('US Census', {}):
            geoids.add(data['coverage']['US Census']['geoid'])
    
    return geoids

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
                ctx.move_to(*points[-1])
            
                for point in points:
                    ctx.line_to(*point)

                ctx.stroke()

def fill_features(ctx, features):
    '''
    '''
    for feature in features:
        geometry = feature.GetGeometryRef()
    
        if geometry.GetGeometryType() == ogr.wkbMultiPolygon:
            parts = geometry
        elif geometry.GetGeometryType() == ogr.wkbPolygon:
            parts = [geometry]
        else:
            raise NotImplementedError()

        for part in parts:
            for ring in part:
                points = ring.GetPoints()
                ctx.move_to(*points[-1])
            
                for point in points:
                    ctx.line_to(*point)

            ctx.fill()

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

    geodata = join(dirname(__file__), 'geodata')
    nation_ds = ogr.Open(join(geodata, 'cb_2013_us_nation_20m-2163.shp'))
    state_ds = ogr.Open(join(geodata, 'cb_2013_us_state_20m-2163.shp'))
    county_ds = ogr.Open(join(geodata, 'cb_2013_us_county_20m-2163.shp'))

    nation_features = list(nation_ds.GetLayer(0))
    state_features = list(state_ds.GetLayer(0))
    county_features = list(county_ds.GetLayer(0))
    data_states = [f for f in state_features if f.GetFieldAsString('GEOID') in geoids]
    data_counties = [f for f in county_features if f.GetFieldAsString('GEOID') in geoids]
    
    # Draw each border between neighboring states exactly once.
    state_borders = [s1.GetGeometryRef().Intersection(s2.GetGeometryRef())
                     for (s1, s2) in combinations(state_features, 2)
                     if s1.GetGeometryRef().Intersects(s2.GetGeometryRef())]
    
    # Fill nation background
    context.set_source_rgb(0xdd/0xff, 0xdd/0xff, 0xdd/0xff)
    fill_features(context, nation_features)

    # Fill populated states
    context.set_source_rgb(0x74/0xff, 0xA5/0xff, 0x78/0xff)
    fill_features(context, data_states)

    # Fill populated counties
    context.set_source_rgb(0x1C/0xff, 0x89/0xff, 0x3F/0xff)
    fill_features(context, data_counties)

    # Outline states and nation
    context.set_source_rgb(0, 0, 0)
    context.set_line_width(.5 * resolution / scale)
    stroke_geometries(context, state_borders)
    context.set_line_width(1 * resolution / scale)
    stroke_features(context, nation_features)

    # Output
    surface.write_to_png(filename)

if __name__ == '__main__':
    exit(main())
