from zipfile import ZipFile
from io import TextIOWrapper
from csv import DictReader
from math import pow, sqrt

from osgeo import osr, ogr

# WGS 84, http://spatialreference.org/ref/epsg/4326/
EPSG4326 = '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs'

# Web Mercator, http://spatialreference.org/ref/sr-org/6864/
EPSG3857 = '+proj=merc +lon_0=0 +k=1 +x_0=0 +y_0=0 +a=6378137 +b=6378137 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs'

def main():
    '''
    '''
    lonlats = iterate_zipfile_points('us-ca-berkeley.zip')
    points = project_points(lonlats)
    xs, ys = zip(*points)
    
    (xmean, xsdev), (ymean, ysdev) = stats(xs), stats(ys)
    xmin, xmax = xmean - 3 * xsdev, xmean + 3 * xsdev
    ymin, ymax = ymean - 3 * ysdev, ymean + 3 * ysdev
    
    osr.UseExceptions()
    sref_geo = osr.SpatialReference(); sref_geo.ImportFromProj4(EPSG4326)
    sref_map = osr.SpatialReference(); sref_map.ImportFromProj4(EPSG3857)
    project = osr.CoordinateTransformation(sref_map, sref_geo)
    cen = ogr.CreateGeometryFromWkt('POINT({:.7f} {:.7f})'.format(xmean, ymean))
    cen.Transform(project)
    print(cen)
    sw = ogr.CreateGeometryFromWkt('POINT({:.7f} {:.7f})'.format(xmin, ymin))
    sw.Transform(project)
    print(sw)
    ne = ogr.CreateGeometryFromWkt('POINT({:.7f} {:.7f})'.format(xmax, ymax))
    ne.Transform(project)
    print(ne)

    okay_xs = [x for (x, y) in points if (xmin <= x <= xmax)]
    okay_ys = [y for (x, y) in points if (ymin <= y <= ymax)]
    
    
    
    in_bounds = [(x, y) for (x, y) in points
                 if (xmin <= x <= xmax and ymin <= y <= ymax)]
    
    print(points[:3], '\n', xs[:3], ys[:3], '\n', (xmean, xsdev), (ymean, ysdev), '\n', (xmin, ymin), (xmax, ymax))
    print(len(points), 'points with', len(in_bounds), 'in bounds')

def iterate_zipfile_points(filename):
    '''
    '''
    with open(filename, 'rb') as file:
        zip = ZipFile(file)
        csv_names = [name for name in zip.namelist() if name.endswith('.csv')]
        csv_file = TextIOWrapper(zip.open(csv_names[0]))
        
        for row in DictReader(csv_file):
            try:
                lon, lat = float(row['LON']), float(row['LAT'])
            except:
                continue
            
            if -180 <= lon <= 180 and -90 <= lat <= 90:
                yield (lon, lat)

def project_points(lonlats):
    '''
    '''
    osr.UseExceptions()
    sref_geo = osr.SpatialReference(); sref_geo.ImportFromProj4(EPSG4326)
    sref_map = osr.SpatialReference(); sref_map.ImportFromProj4(EPSG3857)
    project = osr.CoordinateTransformation(sref_geo, sref_map)
    
    points = list()
    
    for (lon, lat) in lonlats:
        geom = ogr.CreateGeometryFromWkt('POINT({:.7f} {:.7f})'.format(lon, lat))
        geom.Transform(project)
        points.append((geom.GetX(), geom.GetY()))
    
    return points

def stats(values):
    '''
    '''
    mean = sum(values) / len(values)
    deviations = [pow(val - mean, 2) for val in values]
    stddev = sqrt(sum(deviations) / len(values))

    return mean, stddev

if __name__ == '__main__':
    exit(main())
