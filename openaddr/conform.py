import os
import errno
import tempfile
import unicodecsv
import json
import copy

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

    def convert(self, source_definition, source_paths, workdir):
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

            rc = conform_cli(source_definition, source_path, file_path)
            if rc == 0:
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


### Row-level conform code. Inputs and outputs are individual rows in a CSV file.
### The input row may or may not be modified in place. The output row is always returned.

def row_transform_and_convert(sd, row):
    "Apply the full conform transform and extract operations to a row"

    # Some conform specs have fields named with a case different from the source
    row = row_smash_case(sd, row)

    c = sd["conform"]
    if c.has_key("merge"):
        row = row_merge_street(sd, row)
    if c.has_key("advanced_merge"):
        row = row_advanced_merge(sd, row)
    if c.has_key("split"):
        row = row_split_address(sd, row)
    # TODO: expand abbreviations? Node code does, but seems like a bad idea
    row = row_convert_to_out(sd, row)
    return row

def conform_smash_case(source_definition):
    "Convert all named fields in source_definition object to lowercase. Returns new object."
    new_sd = copy.deepcopy(source_definition)
    conform = new_sd["conform"]
    for k in ("split", "lat", "lon", "street", "number"):
        if conform.has_key(k):
            conform[k] = conform[k].lower()
    if conform.has_key("merge"):
        conform["merge"] = [s.lower() for s in conform["merge"]]
    return new_sd

def row_smash_case(sd, row):
    "Convert all field names to lowercase. Slow, but necessary for imprecise conform specs."
    row = { k.lower(): v for (k, v) in row.items() }
    return row

def row_merge_street(sd, row):
    "Merge multiple columns like 'Maple','St' to 'Maple St'"
    merge_data = [row[field] for field in sd["conform"]["merge"]]
    row['auto_street'] = ' '.join(merge_data)
    return row

def row_advanced_merge(sd, row):
    assert False

def row_split_address(sd, row):
    "Split addresses like '123 Maple St' into '123' and 'Maple St'"
    cols = row[sd["conform"]["split"]].split(' ', 1)  # maxsplit
    row['auto_number'] = cols[0]
    row['auto_street'] = cols[1] if len(cols) > 1 else ''
    return row

def row_convert_to_out(sd, row):
    "Convert a row from the source schema to OpenAddresses output schema"
    return {
        "LON": row.get(sd["conform"]["lon"], None),
        "LAT": row.get(sd["conform"]["lat"], None),
        "NUMBER": row.get(sd["conform"]["number"], None),
        "STREET": row.get(sd["conform"]["street"], None)
    }

### File-level conform code. Inputs and outputs are filenames.

def extract_to_source_csv(source_definition, source_path, extract_path):
    """Extract arbitrary downloaded sources to an extracted CSV in the source schema.
    source_definition: description of the source, containing the conform object
    extract_path: file to write the extracted CSV file

    The extracted file will be in UTF-8 and will have X and Y columns corresponding
    to longitude and latitude in EPSG:4326.
    """
    # TODO: handle non-SHP sources
    assert (source_definition["conform"]["type"] == "shapefile" or
            source_definition["conform"]["type"] == "shapefile-polygon")
    shp_to_csv(source_path, extract_path)

# The canonical output schema for conform
_openaddr_csv_schema = ["LON", "LAT", "NUMBER", "STREET"]

def transform_to_out_csv(source_definition, extract_path, dest_path):
    """Transform an extracted source CSV to the OpenAddresses output CSV by applying conform rules
    source_definition: description of the source, containing the conform object
    extract_path: extracted CSV file to process
    dest_path: path for output file in OpenAddress CSV
    """

    # Convert all field names in the conform spec to lower case
    source_definition = conform_smash_case(source_definition)

    # Read through the extract CSV
    with open(extract_path, 'rb') as extract_fp:
        reader = unicodecsv.DictReader(extract_fp, encoding='utf-8')
        # Write to the destination CSV
        with open(dest_path, 'wb') as dest_fp:
            writer = unicodecsv.DictWriter(dest_fp, _openaddr_csv_schema)
            writer.writeheader()
            # For every row in the extract
            for extract_row in reader:
                out_row = row_transform_and_convert(source_definition, extract_row)
                writer.writerow(out_row)

def conform_cli(source_definition, source_path, dest_path):
    "Command line entry point for conforming a downloaded source to an output CSV."
    # TODO: this tool only works if the source creates a single output

    logger = getLogger('openaddr')
    if not source_definition.has_key("conform"):
        return 1
    if not source_definition["conform"].get("type", None) in ["shapefile", "shapefile-polygon"]:
        logger.warn("Skipping file with unknown conform: %s", source_path)
        return 1

    # Create a temporary filename for the intermediate extracted source CSV
    fd, extract_path = tempfile.mkstemp(prefix='openaddr-extracted-', suffix='.csv')
    os.close(fd)
    getLogger('openaddr').debug('extract temp file %s', extract_path)

    try:
        extract_to_source_csv(source_definition, source_path, extract_path)
        transform_to_out_csv(source_definition, extract_path, dest_path)
    finally:
        os.remove(extract_path)

    return 0

def main():
    "Main entry point for openaddr-pyconform command line tool. (See setup.py)"

    parser = ArgumentParser(description='Conform a downloaded source file.')
    parser.add_argument('source_json', help='Required source JSON file name.')
    parser.add_argument('source_path', help='Required pathname to the actual source data file')
    parser.add_argument('dest_path', help='Required pathname, output file written here.')
    parser.add_argument('-l', '--logfile', help='Optional log file name.')
    args = parser.parse_args()

    from .jobs import setup_logger
    setup_logger(args.logfile)

    source_definition = json.load(file(args.source_json))
    rc = conform_cli(source_definition, args.source_path, args.dest_path)
    return rc

if __name__ == '__main__':
    exit(main())


# Test suite. This code could be in a separate file

import unittest, tempfile, shutil

class TestPyConformTransforms (unittest.TestCase):
    "Test low level data transform functions"

    def test_row_smash_case(self):
        r = row_smash_case(None, {"UPPER": "foo", "lower": "bar", "miXeD": "mixed"})
        self.assertEqual({"upper": "foo", "lower": "bar", "mixed": "mixed"}, r)

    def test_conform_smash_case(self):
        d = { "conform": { "street": "MiXeD", "number": "U", "split": "U", "merge": [ "U", "l", "MiXeD" ], "lat": "Y", "lon": "x" } }
        r = conform_smash_case(d)
        self.assertEqual({ "conform": { "street": "mixed", "number": "u", "split": "u", "merge": [ "u", "l", "mixed" ], "lat": "y", "lon": "x" } }, r)

    def test_row_convert_to_out(self):
        d = { "conform": { "street": "s", "number": "n", "lon": "Y", "lat": "X" } }
        r = row_convert_to_out(d, {"s": "MAPLE LN", "n": "123", "Y": "-119.2", "X": "39.3"})
        self.assertEqual({"LON": "-119.2", "LAT": "39.3", "STREET": "MAPLE LN", "NUMBER": "123"}, r)

    def test_row_merge_street(self):
        d = { "conform": { "merge": [ "n", "t" ] } }
        r = row_merge_street(d, {"n": "MAPLE", "t": "ST", "x": "foo"})
        self.assertEqual({"auto_street": "MAPLE ST", "x": "foo", "t": "ST", "n": "MAPLE"}, r)

    def test_split_address(self):
        d = { "conform": { "split": "ADDRESS" } }
        r = row_split_address(d, { "ADDRESS": "123 MAPLE ST" })
        self.assertEqual({"ADDRESS": "123 MAPLE ST", "auto_street": "MAPLE ST", "auto_number": "123"}, r)
        r = row_split_address(d, { "ADDRESS": "265" })
        self.assertEqual(r["auto_number"], "265")
        self.assertEqual(r["auto_street"], "")
        r = row_split_address(d, { "ADDRESS": "" })
        self.assertEqual(r["auto_number"], "")
        self.assertEqual(r["auto_street"], "")

    def test_transform_and_convert(self):
        d = { "conform": { "street": "auto_street", "number": "n", "merge": ["s1", "s2"], "lon": "y", "lat": "x" } }
        r = row_transform_and_convert(d, { "n": "123", "s1": "MAPLE", "s2": "ST", "Y": "-119.2", "X": "39.3" })
        self.assertEqual({"STREET": "MAPLE ST", "NUMBER": "123", "LON": "-119.2", "LAT": "39.3"}, r)

        d = { "conform": { "street": "auto_street", "number": "auto_number", "split": "s", "lon": "y", "lat": "x" } }
        r = row_transform_and_convert(d, { "s": "123 MAPLE ST", "Y": "-119.2", "X": "39.3" })
        self.assertEqual({"STREET": "MAPLE ST", "NUMBER": "123", "LON": "-119.2", "LAT": "39.3"}, r)



class TestPyConformCli (unittest.TestCase):
    "Test the command line interface creates valid output files from test input"
    def setUp(self):
        from . jobs import setup_logger
        setup_logger(False)
        self.testdir = tempfile.mkdtemp(prefix='openaddr-testPyConformCli-')
        self.conforms_dir = os.path.join(os.path.dirname(__file__), '..', 'tests', 'conforms')

    def tearDown(self):
        shutil.rmtree(self.testdir)

    def _source_definition(self, filename):
        "Load source definition object from test fixture"
        return json.load(file(os.path.join(self.conforms_dir, filename))) 

    def _source_path(self, filename):
        "Return the path for the source data in the test fixture"
        return os.path.join(self.conforms_dir, filename)

    def test_unknown_conform(self):
        # Test that the conform tool does something reasonable with unknown conform sources
        self.assertEqual(1, conform_cli({}, 'test', ''))
        self.assertEqual(1, conform_cli({'conform': {}}, 'test', ''))
        self.assertEqual(1, conform_cli({'conform': {'type': 'broken'}}, 'test', ''))

    def test_lake_man(self):
        dest_path = os.path.join(self.testdir, 'test_lake_man.csv')

        rc = conform_cli(self._source_definition('lake-man.json'),
                         self._source_path('lake-man.shp'), dest_path)
        self.assertEqual(0, rc)

        with open(dest_path) as fp:
            reader = unicodecsv.DictReader(fp)
            self.assertEqual(['LON', 'LAT', 'NUMBER', 'STREET'], reader.fieldnames)

            rows = list(reader)

            self.assertAlmostEqual(float(rows[0]['LAT']), 37.802612637607439)
            self.assertAlmostEqual(float(rows[0]['LON']), -122.259249687194824)

            self.assertEqual(rows[0]['NUMBER'], '5115')
            self.assertEqual(rows[0]['STREET'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[1]['NUMBER'], '5121')
            self.assertEqual(rows[1]['STREET'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[2]['NUMBER'], '5133')
            self.assertEqual(rows[2]['STREET'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[3]['NUMBER'], '5126')
            self.assertEqual(rows[3]['STREET'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[4]['NUMBER'], '5120')
            self.assertEqual(rows[4]['STREET'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[5]['NUMBER'], '5115')
            self.assertEqual(rows[5]['STREET'], 'OLD MILL RD')

    def test_lake_man_split(self):
        dest_path = os.path.join(self.testdir, 'test_lake_man_split.csv')
        
        rc = conform_cli(self._source_definition('lake-man-split.json'),
                         self._source_path('lake-man-split.shp'), dest_path)
        self.assertEqual(0, rc)
        
        with open(dest_path) as fp:
            rows = list(unicodecsv.DictReader(fp))
            self.assertEqual(rows[0]['NUMBER'], '915')
            self.assertEqual(rows[0]['STREET'], 'EDWARD AVE')
            self.assertEqual(rows[1]['NUMBER'], '3273')
            self.assertEqual(rows[1]['STREET'], 'PETER ST')
            self.assertEqual(rows[2]['NUMBER'], '976')
            self.assertEqual(rows[2]['STREET'], 'FORD BLVD')
            self.assertEqual(rows[3]['NUMBER'], '7055')
            self.assertEqual(rows[3]['STREET'], 'ST ROSE AVE')
            self.assertEqual(rows[4]['NUMBER'], '534')
            self.assertEqual(rows[4]['STREET'], 'WALLACE AVE')
            self.assertEqual(rows[5]['NUMBER'], '531')
            self.assertEqual(rows[5]['STREET'], 'SCOFIELD AVE')

    def test_lake_man_merge_postcode(self):
        dest_path = os.path.join(self.testdir, 'test_lake_man_merge_postcode.csv')
        
        rc = conform_cli(self._source_definition('lake-man-merge-postcode.json'),
                         self._source_path('lake-man-merge-postcode.shp'), dest_path)
        self.assertEqual(0, rc)
        
        with open(dest_path) as fp:
            rows = list(unicodecsv.DictReader(fp))
            self.assertEqual(rows[0]['NUMBER'], '35845')
            self.assertEqual(rows[0]['STREET'], 'EKLUTNA LAKE RD')
            self.assertEqual(rows[1]['NUMBER'], '35850')
            self.assertEqual(rows[1]['STREET'], 'EKLUTNA LAKE RD')
            self.assertEqual(rows[2]['NUMBER'], '35900')
            self.assertEqual(rows[2]['STREET'], 'EKLUTNA LAKE RD')
            self.assertEqual(rows[3]['NUMBER'], '35870')
            self.assertEqual(rows[3]['STREET'], 'EKLUTNA LAKE RD')
            self.assertEqual(rows[4]['NUMBER'], '32551')
            self.assertEqual(rows[4]['STREET'], 'EKLUTNA LAKE RD')
            self.assertEqual(rows[5]['NUMBER'], '31401')
            self.assertEqual(rows[5]['STREET'], 'EKLUTNA LAKE RD')
    
    def test_lake_man_merge_postcode2(self):
        dest_path = os.path.join(self.testdir, 'test_lake_man_merge_postcode2.csv')
        
        rc = conform_cli(self._source_definition('lake-man-merge-postcode2.json'),
                         self._source_path('lake-man-merge-postcode2.shp'), dest_path)
        self.assertEqual(0, rc)
        
        with open(dest_path) as fp:
            rows = list(unicodecsv.DictReader(fp))
            self.assertEqual(rows[0]['NUMBER'], '85')
            self.assertEqual(rows[0]['STREET'], 'MAITLAND DR')
            self.assertEqual(rows[1]['NUMBER'], '81')
            self.assertEqual(rows[1]['STREET'], 'MAITLAND DR')
            self.assertEqual(rows[2]['NUMBER'], '92')
            self.assertEqual(rows[2]['STREET'], 'MAITLAND DR')
            self.assertEqual(rows[3]['NUMBER'], '92')
            self.assertEqual(rows[3]['STREET'], 'MAITLAND DR')
            self.assertEqual(rows[4]['NUMBER'], '92')
            self.assertEqual(rows[4]['STREET'], 'MAITLAND DR')
            self.assertEqual(rows[5]['NUMBER'], '92')
            self.assertEqual(rows[5]['STREET'], 'MAITLAND DR')