import os
import errno
import tempfile
import unicodecsv
import json

from logging import getLogger
from zipfile import ZipFile
from argparse import ArgumentParser

from .sample import sample_geojson

from osgeo import ogr, osr
ogr.UseExceptions()

geometry_types = {
    ogr.wkbPoint: 'Point',
    ogr.wkbPoint25D: 'Point 2.5D',
    ogr.wkbLineString: 'LineString',
    ogr.wkbLineString25D: 'LineString 2.5D',
    ogr.wkbLinearRing: 'LinearRing',
    ogr.wkbPolygon: 'Polygon',
    ogr.wkbPolygon25D: 'Polygon 2.5D',
    ogr.wkbMultiPoint: 'MultiPoint',
    ogr.wkbMultiPoint25D: 'MultiPoint 2.5D',
    ogr.wkbMultiLineString: 'MultiLineString',
    ogr.wkbMultiLineString25D: 'MultiLineString 2.5D',
    ogr.wkbMultiPolygon: 'MultiPolygon',
    ogr.wkbMultiPolygon25D: 'MultiPolygon 2.5D',
    ogr.wkbGeometryCollection: 'GeometryCollection',
    ogr.wkbGeometryCollection25D: 'GeometryCollection 2.5D',
    ogr.wkbUnknown: 'Unknown'
    }

def mkdirsp(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


class ConformResult:
    processed = None
    sample = None
    geometry_type = None
    path = None
    elapsed = None
    
    # needed by openaddr.process.write_state(), for now.
    output = ''

    def __init__(self, processed, sample, geometry_type, path, elapsed):
        self.processed = processed
        self.sample = sample
        self.geometry_type = geometry_type
        self.path = path
        self.elapsed = elapsed

    @staticmethod
    def empty():
        return ConformResult(None, None, None, None, None)

    def todict(self):
        return dict(processed=self.processed, sample=self.sample)


class DecompressionError(Exception):
    pass


class DecompressionTask(object):
    @classmethod
    def from_type_string(clz, type_string):
        if type_string == None:
            return NoopDecompressTask()
        elif type_string.lower() == 'zip':
            return ZipDecompressTask()
        else:
            raise KeyError("I don't know how to decompress for type {}".format(type_string))

    def decompress(self, source_paths):
        raise NotImplementedError()


class NoopDecompressTask(DecompressionTask):
    def decompress(self, source_paths, workdir):
        return source_paths


class ZipDecompressTask(DecompressionTask):

    logger = getLogger('openaddr')

    def decompress(self, source_paths, workdir):
        output_files = []
        expand_path = os.path.join(workdir, 'unzipped')
        mkdirsp(expand_path)

        for source_path in source_paths:
            with ZipFile(source_path, 'r') as z:
                for name in z.namelist():
                    expanded_file_path = z.extract(name, expand_path)
                    self.logger.debug("Expanded file %s", expanded_file_path)
                    output_files.append(expanded_file_path)
        return output_files

class ExcerptDataTask(object):
    ''' Task for sampling three rows of data from datasource.
    '''
    logger = getLogger('openaddr')
    known_types = ('.shp', '.json', '.csv', '.kml')

    def excerpt(self, source_paths, workdir):
        '''
        
            Tested version from openaddr.excerpt() on master branch:

            if ext == '.zip':
                logger.debug('Downloading all of {cache}'.format(**extras))

                with open(cachefile, 'w') as file:
                    for chunk in got.iter_content(1024**2):
                        file.write(chunk)
    
                zf = ZipFile(cachefile, 'r')
        
                for name in zf.namelist():
                    _, ext = splitext(name)
            
                    if ext in ('.shp', '.shx', '.dbf'):
                        with open(join(workdir, 'cache'+ext), 'w') as file:
                            file.write(zf.read(name))
        
                if exists(join(workdir, 'cache.shp')):
                    ds = ogr.Open(join(workdir, 'cache.shp'))
                else:
                    ds = None
    
            elif ext == '.json':
                logger.debug('Downloading part of {cache}'.format(**extras))

                scheme, host, path, query, _, _ = urlparse(got.url)
        
                if scheme in ('http', 'https'):
                    conn = HTTPConnection(host, 80)
                    conn.request('GET', path + ('?' if query else '') + query)
                    resp = conn.getresponse()
                elif scheme == 'file':
                    with open(path) as rawfile:
                        resp = StringIO(rawfile.read(1024*1024))
                else:
                    raise RuntimeError('Unsure what to do with {}'.format(got.url))
        
                with open(cachefile, 'w') as file:
                    file.write(sample_geojson(resp, 10))
    
                ds = ogr.Open(cachefile)
    
            else:
                ds = None
        '''
        known_paths = [source_path for source_path in source_paths
                       if os.path.splitext(source_path)[1] in self.known_types]
        
        if not known_paths:
            # we know nothing.
            return None
        
        data_path = known_paths[0]

        # Sample a few GeoJSON features to save on memory for large datasets.
        if os.path.splitext(data_path)[1] == '.json':
            with open(data_path, 'r') as complete_layer:
                temp_dir = os.path.dirname(data_path)
                _, temp_path = tempfile.mkstemp(dir=temp_dir, suffix='.json')

                with open(temp_path, 'w') as temp_file:
                    temp_file.write(sample_geojson(complete_layer, 10))
                    data_path = temp_path
        
        datasource = ogr.Open(data_path, 0)
        layer = datasource.GetLayer()

        layer_defn = layer.GetLayerDefn()
        fieldnames = [layer_defn.GetFieldDefn(i).GetName()
                      for i in range(layer_defn.GetFieldCount())]

        data_sample = [fieldnames]
        
        for feature in layer:
            data_sample.append([feature.GetField(i) for i
                                in range(layer_defn.GetFieldCount())])

            if len(data_sample) == 6:
                break
        
        geometry_type = geometry_types.get(layer_defn.GetGeomType(), None)

        return data_sample, geometry_type

class ConvertToCsvTask(object):
    logger = getLogger('openaddr')
    known_types = ('.shp', '.json', '.csv', '.kml')

    def convert(self, source_paths, workdir):
        "Convert a list of source_paths and write results in workdir"
        self.logger.debug("Convert {} {}".format(source_paths, workdir))

        # Create a subdirectory "converted" to hold results
        output_files = []
        convert_path = os.path.join(workdir, 'converted')
        mkdirsp(convert_path)

        # For every source path, try converting it
        for source_path in source_paths:
            filename = os.path.basename(source_path)
            basename, ext = os.path.splitext(filename)
            file_path = os.path.join(convert_path, basename + '.csv')

            if ext not in self.known_types:
                self.logger.debug("Skipping %s because I don't know how to convert it", source_path)
                continue
            if os.path.exists(file_path):            # is this ever possible?
                output_files.append(file_path)
                self.logger.debug("File exists %s", file_path)
                continue

            shp_to_csv(source_path, file_path)
            output_files.append(file_path)

        return output_files

def shp_to_csv(source_path, dest_path):
    "Convert a single shapefile in source_path and put it in dest_path"
    logger = getLogger('openaddr')

    in_datasource = ogr.Open(source_path, 0)
    in_layer = in_datasource.GetLayer()
    inSpatialRef = in_layer.GetSpatialRef()

    logger.info("Converting a layer to CSV: %s", in_layer)

    in_layer_defn = in_layer.GetLayerDefn()
    out_fieldnames = []
    for i in range(0, in_layer_defn.GetFieldCount()):
        field_defn = in_layer_defn.GetFieldDefn(i)
        out_fieldnames.append(field_defn.GetName())
    out_fieldnames.append('X')
    out_fieldnames.append('Y')

    outSpatialRef = osr.SpatialReference()
    outSpatialRef.ImportFromEPSG(4326)
    coordTransform = osr.CoordinateTransformation(inSpatialRef, outSpatialRef)

    with open(dest_path, 'wb') as f:
        writer = unicodecsv.DictWriter(f, fieldnames=out_fieldnames, encoding='utf-8')
        writer.writeheader()

        in_feature = in_layer.GetNextFeature()
        while in_feature:
            row = dict()

            for i in range(0, in_layer_defn.GetFieldCount()):
                field_defn = in_layer_defn.GetFieldDefn(i)
                row[field_defn.GetNameRef()] = in_feature.GetField(i)
            geom = in_feature.GetGeometryRef()
            geom.Transform(coordTransform)
            # Calculate the centroid of the geometry and write it as X and Y columns
            centroid = geom.Centroid()
            row['X'] = centroid.GetX()
            row['Y'] = centroid.GetY()

            writer.writerow(row)

            in_feature.Destroy()
            in_feature = in_layer.GetNextFeature()

    in_datasource.Destroy()

def extract_to_source_csv(source_definition, source_path, extract_path):
    """Extract arbitrary downloaded sources to an extracted CSV in the source schema.
    source_definition: description of the source, containing the conform object
    extract_path: file to write the extracted CSV file

    The extracted file will be in UTF-8 and will have X and Y columns corresponding
    to longitude and latitude in EPSG:4326.
    """
    # TODO: handle non-SHP sources
    assert source_definition["conform"]["type"] == "shapefile"
    shp_to_csv(source_path, extract_path)


# The canonical output schema for conform
_openaddr_csv_schema = ["LON", "LAT", "NUMBER", "STREET"]

def transform_to_out_csv(source_definition, extract_path, dest_path):
    """Transform an extracted source CSV to the OpenAddresses output CSV by applying conform rules
    source_definition: description of the source, containing the conform object
    extract_path: extracted CSV file to process
    dest_path: path for output file in OpenAddress CSV
    """

    # Pull the "number" and "street" attribute tags out of the conform object
    number_field_name = source_definition["conform"]["number"]
    street_field_name = source_definition["conform"]["street"]

    # Read through the extract CSV
    with open(extract_path, 'rb') as extract_fp:
        reader = unicodecsv.DictReader(extract_fp, encoding='utf-8')
        # Write to the destination CSV
        with open(dest_path, 'wb') as dest_fp:
            writer = unicodecsv.DictWriter(dest_fp, _openaddr_csv_schema)
            writer.writeheader()
            # For every row in the extract
            for extract_row in reader:
                # Construct a row in the ouput
                out_row = {
                    "LON": extract_row.get("X", None),
                    "LAT": extract_row.get("Y", None),
                    "NUMBER": extract_row.get(number_field_name, None),
                    "STREET": extract_row.get(street_field_name, None)
                }
                writer.writerow(out_row)

def conform_cli(source_definition, source_path, workdir):
    "Command line entry point for conforming."

    # TODO: hardcoded filename is bad. Also won't work with sources containing multiple shapefiles, etc
    extract_path = os.path.join(workdir, 'extracted.csv')
    out_path = os.path.join(workdir, 'out.csv')

    extract_to_source_csv(source_definition, source_path, extract_path)
    transform_to_out_csv(source_definition, extract_path, out_path)

    return 0

parser = ArgumentParser(description='Conform a downloaded source file.')
parser.add_argument('source', help='Required source JSON file name.')
parser.add_argument('source_data', help='Required pathname to the actual source data file')
parser.add_argument('workdir', help='Required directory name. Must contain downloaded source file, out.csv created here.')
parser.add_argument('-l', '--logfile', help='Optional log file name.')

def main():
    from .jobs import setup_logger
    args = parser.parse_args()
    setup_logger(args.logfile)

    source_definition = json.load(file(args.source))
    rc = conform_cli(source_definition, args.source_data, args.workdir)
    return rc

if __name__ == '__main__':
    exit(main())
