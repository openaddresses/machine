# coding=ascii

from __future__ import absolute_import, division, print_function
import logging; _L = logging.getLogger('openaddr.conform')

from .compat import standard_library

import os
import errno
import tempfile
import itertools
import json
import copy
import sys
import re

from zipfile import ZipFile
from argparse import ArgumentParser
from locale import getpreferredencoding
from os.path import splitext
from hashlib import sha1
from uuid import uuid4

from .compat import csvopen, csvreader, csvDictReader, csvDictWriter
from .sample import sample_geojson, stream_geojson

from osgeo import ogr, osr, gdal
ogr.UseExceptions()


def gdal_error_handler(err_class, err_num, err_msg):
    errtype = {
            gdal.CE_None:'None',
            gdal.CE_Debug:'Debug',
            gdal.CE_Warning:'Warning',
            gdal.CE_Failure:'Failure',
            gdal.CE_Fatal:'Fatal'
    }
    err_msg = err_msg.replace('\n',' ')
    err_class = errtype.get(err_class, 'None')
    _L.error("GDAL gave %s %s: %s", err_class, err_num, err_msg)
gdal.PushErrorHandler(gdal_error_handler)


# The canonical output schema for conform
OPENADDR_CSV_SCHEMA = ['LON', 'LAT', 'NUMBER', 'STREET', 'UNIT', 'CITY',
                       'DISTRICT', 'REGION', 'POSTCODE', 'ID', 'HASH']

# Field names for use in cached CSV files.
# We add columns to the extracted CSV with our own data with these names.
GEOM_FIELDNAME = 'OA:geom'
X_FIELDNAME, Y_FIELDNAME = 'OA:x', 'OA:y'
attrib_types = { 
    'street':   'OA:street',
    'number':   'OA:number',
    'unit':     'OA:unit',
    'city':     'OA:city',
    'postcode': 'OA:postcode',
    'district': 'OA:district',
    'region':   'OA:region',
    'id':       'OA:id'
}

UNZIPPED_DIRNAME = 'unzipped'

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

prefixed_number_pattern = re.compile("^\s*([0-9]+)\s+", False)
postfixed_street_pattern = re.compile("^(?:\s*[0-9]+\s+)?(.*)", False)

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
    license = None
    geometry_type = None
    address_count = None
    path = None
    elapsed = None
    sharealike_flag = None
    attribution_flag = None
    attribution_name = None
    
    def __init__(self, processed, sample, website, license, geometry_type,
                 address_count, path, elapsed, sharealike_flag,
                 attribution_flag, attribution_name):
        self.processed = processed
        self.sample = sample
        self.website = website
        self.license = license
        self.geometry_type = geometry_type
        self.address_count = address_count
        self.path = path
        self.elapsed = elapsed
        self.sharealike_flag = sharealike_flag
        self.attribution_flag = attribution_flag
        self.attribution_name = attribution_name

    @staticmethod
    def empty():
        return ConformResult(None, None, None, None, None, None, None, None, None, None, None)

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
    def decompress(self, source_paths, workdir, filenames):
        return source_paths

def is_in(path, names):
    '''
    '''
    if path.lower() in names:
        # Found it!
        return True
    
    for name in names:
        # Maybe one of the names is an enclosing directory?
        if not os.path.relpath(path.lower(), name).startswith('..'):
            # Yes, that's it.
            return True

    return False

class ZipDecompressTask(DecompressionTask):
    def decompress(self, source_paths, workdir, filenames):
        output_files = []
        expand_path = os.path.join(workdir, UNZIPPED_DIRNAME)
        mkdirsp(expand_path)

        # Extract contents of zip file into expand_path directory.
        for source_path in source_paths:
            with ZipFile(source_path, 'r') as z:
                for name in z.namelist():
                    if len(filenames) and not is_in(name, filenames):
                        # Download only the named file, if any.
                        _L.debug("Skipped file {}".format(name))
                        continue
                
                    z.extract(name, expand_path)
        
        # Collect names of directories and files in expand_path directory.
        for (dirpath, dirnames, filenames) in os.walk(expand_path):
            for dirname in dirnames:
                output_files.append(os.path.join(dirpath, dirname))
                _L.debug("Expanded directory {}".format(output_files[-1]))
            for filename in filenames:
                output_files.append(os.path.join(dirpath, filename))
                _L.debug("Expanded file {}".format(output_files[-1]))
        
        return output_files

class ExcerptDataTask(object):
    ''' Task for sampling three rows of data from datasource.
    '''
    known_types = ('.shp', '.json', '.geojson', '.csv', '.kml', '.gml', '.gdb')

    def excerpt(self, source_paths, workdir, conform):
        '''
        
            Tested version from openaddr.excerpt() on master branch:

            if ext == '.zip':
                _L.debug('Downloading all of {cache}'.format(**extras))

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
                _L.debug('Downloading part of {cache}'.format(**extras))

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
        encoding = conform.get('encoding')
        csvsplit = conform.get('csvsplit', ',')
        
        known_paths = ExcerptDataTask._get_known_paths(source_paths, workdir, conform, self.known_types)
        
        if not known_paths:
            # we know nothing.
            return None, None

        data_path = known_paths[0]
        _, data_ext = os.path.splitext(data_path.lower())

        # Sample a few GeoJSON features to save on memory for large datasets.
        if data_ext in ('.geojson', '.json'):
            data_path = ExcerptDataTask._sample_geojson_file(data_path)
        
        # GDAL has issues with weird input CSV data, so use Python instead.
        if conform.get('type') == 'csv':
            return ExcerptDataTask._excerpt_csv_file(data_path, encoding, csvsplit)

        ogr_data_path = normalize_ogr_filename_case(data_path)
        datasource = ogr.Open(ogr_data_path, 0)
        layer = datasource.GetLayer()

        if not encoding:
            encoding = guess_source_encoding(datasource, layer)
        
        # GDAL has issues with non-UTF8 input CSV data, so use Python instead.
        if data_ext == '.csv' and encoding not in ('utf8', 'utf-8'):
            return ExcerptDataTask._excerpt_csv_file(data_path, encoding, csvsplit)
        
        layer_defn = layer.GetLayerDefn()
        fieldcount = layer_defn.GetFieldCount()
        fieldnames = [layer_defn.GetFieldDefn(i).GetName() for i in range(fieldcount)]
        fieldnames = [f.decode(encoding) if hasattr(f, 'decode') else f for f in fieldnames]

        data_sample = [fieldnames]
        
        for (feature, _) in zip(layer, range(5)):
            row = [feature.GetField(i) for i in range(fieldcount)]
            row = [v.decode(encoding) if hasattr(v, 'decode') else v for v in row]
            data_sample.append(row)

        if len(data_sample) < 2:
            raise ValueError('Not enough rows in data source')
        
        # Determine geometry_type from layer, sample, or give up.
        if layer_defn.GetGeomType() in geometry_types:
            geometry_type = geometry_types.get(layer_defn.GetGeomType(), None)
        elif fieldnames[-3:] == [X_FIELDNAME, Y_FIELDNAME, GEOM_FIELDNAME]:
            geometry = ogr.CreateGeometryFromWkt(data_sample[1][-1])
            geometry_type = geometry_types.get(geometry.GetGeometryType(), None)
        else:
            geometry_type = None

        return data_sample, geometry_type

    @staticmethod
    def _get_known_paths(source_paths, workdir, conform, known_types):
        if conform.get('type') != 'csv' or 'file' not in conform:
            paths = [source_path for source_path in source_paths
                     if os.path.splitext(source_path)[1].lower() in known_types]
            
            # If nothing was found or named but we expect a CSV, return first file.
            if not paths and conform.get('type') == 'csv' and 'file' not in conform:
                return source_paths[:1]
            
            return paths
    
        unzipped_base = os.path.join(workdir, UNZIPPED_DIRNAME)
        unzipped_paths = dict([(os.path.relpath(source_path, unzipped_base), source_path)
                               for source_path in source_paths])
        
        if conform['file'] not in unzipped_paths:
            return []
        
        csv_path = ExcerptDataTask._make_csv_path(unzipped_paths.get(conform['file']))
        return [csv_path]

    @staticmethod
    def _make_csv_path(csv_path):
        _, csv_ext = os.path.splitext(csv_path.lower())

        if csv_ext != '.csv':
            # Convince OGR it's looking at a CSV file.
            new_path = csv_path + '.csv'
            os.link(csv_path, new_path)
            csv_path = new_path
        
        return csv_path

    @staticmethod
    def _sample_geojson_file(data_path):
        # Sample a few GeoJSON features to save on memory for large datasets.
        with open(data_path, 'r') as complete_layer:
            temp_dir = os.path.dirname(data_path)
            _, temp_path = tempfile.mkstemp(dir=temp_dir, suffix='.json')

            with open(temp_path, 'w') as temp_file:
                temp_file.write(sample_geojson(complete_layer, 10))
                return temp_path
    
    @staticmethod
    def _excerpt_csv_file(data_path, encoding, csvsplit):
        with csvopen(data_path, 'r', encoding=encoding) as file:
            input = csvreader(file, encoding=encoding, delimiter=csvsplit)
            data_sample = [row for (row, _) in zip(input, range(6))]

            if len(data_sample) >= 2 and GEOM_FIELDNAME in data_sample[0]:
                geom_index = data_sample[0].index(GEOM_FIELDNAME)
                geometry = ogr.CreateGeometryFromWkt(data_sample[1][geom_index])
                geometry_type = geometry_types.get(geometry.GetGeometryType(), None)
            else:
                geometry_type = None

        return data_sample, geometry_type

def elaborate_filenames(filename):
    ''' Return a list of filenames for a single name from conform file tag.
    
        Used to expand example.shp with example.shx, example.dbf, and example.prj.
    '''
    if filename is None:
        return []
    
    filename = filename.lower()
    base, original_ext = splitext(filename)
    
    if original_ext == '.shp':
        return [base + ext for ext in (original_ext, '.shx', '.dbf', '.prj')]
    
    return [filename]

def guess_source_encoding(datasource, layer):
    ''' Guess at a string encoding using hints from OGR and locale().
    
        Duplicate the process used in Fiona, described and implemented here:
        
        https://github.com/openaddresses/machine/issues/42#issuecomment-69693143
        https://github.com/Toblerity/Fiona/blob/53df35dc70fb/docs/encoding.txt
        https://github.com/Toblerity/Fiona/blob/53df35dc70fb/fiona/ogrext.pyx#L386
    '''
    ogr_recoding = layer.TestCapability(ogr.OLCStringsAsUTF8)
    is_shapefile = datasource.GetDriver().GetName() == 'ESRI Shapefile'
    
    return (ogr_recoding and 'UTF-8') \
        or (is_shapefile and 'ISO-8859-1') \
        or getpreferredencoding()

def find_source_path(source_definition, source_paths):
    ''' Figure out which of the possible paths is the actual source
    '''
    try:
        conform = source_definition["conform"]
    except KeyError:
        _L.warning('Source is missing a conform object')
        raise

    if conform["type"] in ("shapefile", "shapefile-polygon"):
        # TODO this code is too complicated; see XML variant below for simpler option
        # Shapefiles are named *.shp
        candidates = []
        for fn in source_paths:
            basename, ext = os.path.splitext(fn)
            if ext.lower() == ".shp":
                candidates.append(fn)
        if len(candidates) == 0:
            _L.warning("No shapefiles found in %s", source_paths)
            return None
        elif len(candidates) == 1:
            _L.debug("Selected %s for source", candidates[0])
            return candidates[0]
        else:
            # Multiple candidates; look for the one named by the file attribute
            if "file" not in conform:
                _L.warning("Multiple shapefiles found, but source has no file attribute.")
                return None
            source_file_name = conform["file"]
            for c in candidates:
                if source_file_name == os.path.basename(c):
                    return c
            _L.warning("Source names file %s but could not find it", source_file_name)
            return None
    elif conform["type"] == "geojson" and source_definition["type"] != "ESRI":
        candidates = []
        for fn in source_paths:
            basename, ext = os.path.splitext(fn)
            if ext.lower() in (".json", ".geojson"):
                candidates.append(fn)
        if len(candidates) == 0:
            _L.warning("No JSON found in %s", source_paths)
            return None
        elif len(candidates) == 1:
            _L.debug("Selected %s for source", candidates[0])
            return candidates[0]
        else:
            _L.warning("Found more than one JSON file in source, can't pick one")
            # geojson spec currently doesn't include a file attribute. Maybe it should?
            return None
    elif conform["type"] == "geojson" and source_definition["type"] == "ESRI":
        # Old style ESRI conform: ESRI downloader should only give us a single cache.csv file
        return source_paths[0]
    elif conform["type"] == "csv":
        # Return file if it's specified, else return the first file we find
        if "file" in conform:
            for fn in source_paths:
                # Consider it a match if the basename matches; directory names are a mess
                if os.path.basename(conform["file"]) == os.path.basename(fn):
                    return fn
            _L.warning("Conform named %s as file but we could not find it." % conform["file"])
            return None
        else:
            return source_paths[0]
    elif conform["type"] == "gdb":
        candidates = []
        for fn in source_paths:
            fn = re.sub('\.gdb.*', '.gdb', fn)
            basename, ext = os.path.splitext(fn)
            if ext.lower() == ".gdb" and fn not in candidates:
                candidates.append(fn)
        if len(candidates) == 0:
            _L.warning("No GDB found in %s", source_paths)
            return None
        elif len(candidates) == 1:
            _L.debug("Selected %s for source", candidates[0])
            return candidates[0]
        else: 
            # Multiple candidates; look for the one named by the file attribute
            if "file" not in conform:
                _L.warning("Multiple GDBs found, but source has no file attribute.")
                return None
            source_file_name = conform["file"]
            for c in candidates:
                if source_file_name == os.path.basename(c):
                    return c
            _L.warning("Source names file %s but could not find it", source_file_name)
            return None
    elif conform["type"] == "xml":
        # Return file if it's specified, else return the first .gml file we find
        if "file" in conform:
            for fn in source_paths:
                # Consider it a match if the basename matches; directory names are a mess
                if os.path.basename(conform["file"]) == os.path.basename(fn):
                    return fn
            _L.warning("Conform named %s as file but we could not find it." % conform["file"])
            return None
        else:
            for fn in source_paths:
                _, ext = os.path.splitext(fn)
                if ext == ".gml":
                    return fn
            _L.warning("Could not find a .gml file")
            return None
    else:
        _L.warning("Unknown source conform type %s", conform["type"])
        return None

class ConvertToCsvTask(object):
    known_types = ('.shp', '.json', '.csv', '.kml', '.gdb')

    def convert(self, source_definition, source_paths, workdir):
        "Convert a list of source_paths and write results in workdir"
        _L.debug("Converting to %s", workdir)

        # Create a subdirectory "converted" to hold results
        output_file = None
        convert_path = os.path.join(workdir, 'converted')
        mkdirsp(convert_path)

        # Find the source and convert it
        source_path = find_source_path(source_definition, source_paths)
        if source_path is not None:
            basename, ext = os.path.splitext(os.path.basename(source_path))
            dest_path = os.path.join(convert_path, basename + ".csv")
            rc = conform_cli(source_definition, source_path, dest_path)
            if rc == 0:
                with open(dest_path) as file:
                    addr_count = sum(1 for line in file) - 1

                # Success! Return the path of the output CSV
                return dest_path, addr_count

        # Conversion must have failed
        return None, 0

def convert_regexp_replace(replace):
    ''' Convert regular expression replace string from $ syntax to slash-syntax.
        
        Replace one kind of replacement, then call self recursively to find others.
    '''
    if re.search(r'\$\d+\b', replace):
        # $dd* back-reference followed by a word break.
        return convert_regexp_replace(re.sub(r'\$(\d+)\b', r'\\\g<1>', replace))
    
    if re.search(r'\$\d+\D', replace):
        # $dd* back-reference followed by an non-digit character.
        return convert_regexp_replace(re.sub(r'\$(\d+)(\D)', r'\\\g<1>\g<2>', replace))
    
    if re.search(r'\$\{\d+\}', replace):
        # ${dd*} back-reference.
        return convert_regexp_replace(re.sub(r'\$\{(\d+)\}', r'\\g<\g<1>>', replace))
    
    return replace    

def normalize_ogr_filename_case(source_path):
    '''
    '''
    base, ext = splitext(source_path)
    
    if ext == ext.lower():
        # Extension is already lowercase, no need to do anything.
        return source_path

    normal_path = base + ext.lower()
    
    if os.path.exists(normal_path):
        # We appear to be on a case-insensitive filesystem.
        return normal_path

    os.link(source_path, normal_path)
    
    # May need to deal with some additional files.
    extras = {'.Shp': ('.Shx', '.Dbf', '.Prj'), '.SHP': ('.SHX', '.DBF', '.PRJ')}
    
    if ext in extras:
        for other_ext in extras[ext]:
            if os.path.exists(base + other_ext):
                os.link(base + other_ext, base + other_ext.lower())

    return normal_path

def ogr_source_to_csv(source_definition, source_path, dest_path):
    "Convert a single shapefile or GeoJSON in source_path and put it in dest_path"
    in_datasource = ogr.Open(source_path, 0)
    in_layer = in_datasource.GetLayer()
    inSpatialRef = in_layer.GetSpatialRef()

    _L.info("Converting a layer to CSV: %s", in_layer.GetName())

    # Determine the appropriate SRS
    if inSpatialRef is None:
        # OGR couldn't find the projection, let's hope there's an SRS tag.
        _L.info("No projection file found for source %s", source_path)
        srs = source_definition["conform"].get("srs", None)
        if srs is not None and srs.startswith(u"EPSG:"):
            _L.debug("SRS tag found specifying %s", srs)
            inSpatialRef = osr.SpatialReference()
            inSpatialRef.ImportFromEPSG(int(srs[5:]))
        else:
            # OGR is capable of doing more than EPSG, but so far we don't need it.
            raise Exception("Bad SRS. Can only handle EPSG, the SRS tag is %s", srs)

    # Determine the appropriate text encoding. This is complicated in OGR, see
    # https://github.com/openaddresses/machine/issues/42
    if in_layer.TestCapability(ogr.OLCStringsAsUTF8):
        # OGR turned this to UTF 8 for us
        shp_encoding = 'utf-8'
    elif "encoding" in source_definition["conform"]:
        shp_encoding = source_definition["conform"]["encoding"]
    else:
        _L.warning("No encoding given and OGR couldn't guess. Trying ISO-8859-1, YOLO!")
        shp_encoding = "iso-8859-1"
    _L.debug("Assuming shapefile data is encoded %s", shp_encoding)

    # Get the input schema, create an output schema
    in_layer_defn = in_layer.GetLayerDefn()
    out_fieldnames = []
    for i in range(0, in_layer_defn.GetFieldCount()):
        field_defn = in_layer_defn.GetFieldDefn(i)
        out_fieldnames.append(field_defn.GetName())
    out_fieldnames.append(X_FIELDNAME)
    out_fieldnames.append(Y_FIELDNAME)

    # Set up a transformation from the source SRS to EPSG:4326
    outSpatialRef = osr.SpatialReference()
    outSpatialRef.ImportFromEPSG(4326)
    coordTransform = osr.CoordinateTransformation(inSpatialRef, outSpatialRef)

    # Write a CSV file with one row per feature in the OGR source
    with csvopen(dest_path, 'w', encoding='utf-8') as f:
        writer = csvDictWriter(f, fieldnames=out_fieldnames, encoding='utf-8')
        writer.writeheader()

        in_feature = in_layer.GetNextFeature()
        while in_feature:
            row = dict()

            for i in range(0, in_layer_defn.GetFieldCount()):
                field_defn = in_layer_defn.GetFieldDefn(i)
                field_value = in_feature.GetField(i)
                if isinstance(field_value, str):
                    # Convert OGR's byte sequence strings to Python Unicode strings
                    field_value = field_value.decode(shp_encoding) \
                        if hasattr(field_value, 'decode') else field_value
                row[field_defn.GetNameRef()] = field_value
            geom = in_feature.GetGeometryRef()
            if geom is not None:
                geom.Transform(coordTransform)
                # Calculate the centroid of the geometry and write it as X and Y columns
                try:
                    centroid = geom.Centroid()
                except RuntimeError as e:
                    if 'Invalid number of points in LinearRing found' not in str(e):
                        raise
                    xmin, xmax, ymin, ymax = geom.GetEnvelope()
                    row[X_FIELDNAME] = xmin/2 + xmax/2
                    row[Y_FIELDNAME] = ymin/2 + ymax/2
                else:
                    row[X_FIELDNAME] = centroid.GetX()
                    row[Y_FIELDNAME] = centroid.GetY()
            else:
                row[X_FIELDNAME] = None
                row[Y_FIELDNAME] = None

            writer.writerow(row)

            in_feature.Destroy()
            in_feature = in_layer.GetNextFeature()

    in_datasource.Destroy()

def csv_source_to_csv(source_definition, source_path, dest_path):
    "Convert a source CSV file to an intermediate form, coerced to UTF-8 and EPSG:4326"
    _L.info("Converting source CSV %s", source_path)

    # Encoding processing tag
    enc = source_definition["conform"].get("encoding", "utf-8")

    # csvsplit processing tag
    delim = source_definition["conform"].get("csvsplit", ",")

    # Extract the source CSV, applying conversions to deal with oddball CSV formats
    # Also convert encoding to utf-8 and reproject to EPSG:4326 in X and Y columns
    with csvopen(source_path, 'r', encoding=enc) as source_fp:
        in_fieldnames = None   # in most cases, we let the csv module figure these out

        # headers processing tag
        if "headers" in source_definition["conform"]:
            headers = source_definition["conform"]["headers"]
            if (headers == -1):
                # Read a row off the file to see how many columns it has
                temp_reader = csvreader(source_fp, encoding=enc, delimiter=str(delim))
                first_row = next(temp_reader)
                num_columns = len(first_row)
                source_fp.seek(0)
                in_fieldnames = ["COLUMN%d" % n for n in range(1, num_columns+1)]
                _L.debug("Synthesized header %s", in_fieldnames)
            else:
                # partial implementation of headers and skiplines,
                # matches the sources in our collection as of January 2015
                # this code handles the case for Korean inputs where there are
                # two lines of headers and we want to skip the first one
                assert "skiplines" in source_definition["conform"]
                assert source_definition["conform"]["skiplines"] == headers
                # Skip N lines to get to the real header. headers=2 means we skip one line
                for n in range(1, headers):
                    next(source_fp)
        else:
            # check the source doesn't specify skiplines without headers
            assert "skiplines" not in source_definition["conform"]

        reader = csvDictReader(source_fp, encoding=enc, delimiter=delim, fieldnames=in_fieldnames)
        num_fields = len(reader.fieldnames)

        # Construct headers for the extracted CSV file
        if source_definition["type"] == "ESRI":
            # ESRI sources: just copy what the downloader gave us. (Already has OA:x and OA:y)
            out_fieldnames = list(reader.fieldnames)
        else:
            # CSV sources: replace the source's lat/lon columns with OA:x and OA:y
            old_latlon = [source_definition["conform"]["lat"], source_definition["conform"]["lon"]]
            old_latlon.extend([s.upper() for s in old_latlon])
            out_fieldnames = [fn for fn in reader.fieldnames if fn not in old_latlon]
            out_fieldnames.append(X_FIELDNAME)
            out_fieldnames.append(Y_FIELDNAME)

        # Write the extracted CSV file
        with csvopen(dest_path, 'w', encoding='utf-8') as dest_fp:
            writer = csvDictWriter(dest_fp, out_fieldnames)
            writer.writeheader()
            # For every row in the source CSV
            row_number = 0
            for source_row in reader:
                row_number += 1
                if len(source_row) != num_fields:
                    _L.debug("Skipping row. Got %d columns, expected %d", len(source_row), num_fields)
                    continue
                try:
                    out_row = row_extract_and_reproject(source_definition, source_row)
                except Exception as e:
                    _L.error('Error in row {}: {}'.format(row_number, e))
                    raise
                else:
                    writer.writerow(out_row)

def geojson_source_to_csv(source_path, dest_path):
    '''
    '''
    # For every row in the source GeoJSON
    with open(source_path) as file:
        # Write the extracted CSV file
        with csvopen(dest_path, 'w', encoding='utf-8') as dest_fp:
            writer = None
            for (row_number, feature) in enumerate(stream_geojson(file)):
                if writer is None:
                    out_fieldnames = list(feature['properties'].keys())
                    out_fieldnames.extend((X_FIELDNAME, Y_FIELDNAME))
                    writer = csvDictWriter(dest_fp, out_fieldnames)
                    writer.writeheader()
                
                try:
                    row = feature['properties']
                    geom = ogr.CreateGeometryFromJson(json.dumps(feature['geometry']))
                    if not geom:
                        continue
                    center = geom.Centroid()
                except Exception as e:
                    _L.error('Error in row {}: {}'.format(row_number, e))
                    raise
                else:
                    row.update({X_FIELDNAME: center.GetX(), Y_FIELDNAME: center.GetY()})
                    writer.writerow(row)

_transform_cache = {}
def _transform_to_4326(srs):
    "Given a string like EPSG:2913, return an OGR transform object to turn it in to EPSG:4326"
    if srs not in _transform_cache:
        epsg_id = int(srs[5:]) if srs.startswith("EPSG:") else int(srs)
        # Manufacture a transform object if it's not in the cache
        in_spatial_ref = osr.SpatialReference()
        in_spatial_ref.ImportFromEPSG(epsg_id)
        out_spatial_ref = osr.SpatialReference()
        out_spatial_ref.ImportFromEPSG(4326)
        _transform_cache[srs] = osr.CoordinateTransformation(in_spatial_ref, out_spatial_ref)
    return _transform_cache[srs]

def row_extract_and_reproject(source_definition, source_row):
    ''' Find lat/lon in source CSV data and store it in ESPG:4326 in X/Y in the row
    '''
    # Ignore any lat/lon names for natively geographic sources.
    ignore_conform_names = bool(source_definition['conform']['type'] != 'csv')

    # ESRI-derived source CSV is synthetic; we should ignore any lat/lon names.
    ignore_conform_names |= bool(source_definition['type'] == 'ESRI')

    # Set local variables lon_name, source_x, lat_name, source_y
    if ignore_conform_names:
        # Use our own X_FIELDNAME convention
        lat_name = Y_FIELDNAME
        lon_name = X_FIELDNAME
        source_x = source_row[lon_name]
        source_y = source_row[lat_name]
    else:
        # Conforms can name the lat/lon columns from the original source data
        lat_name = source_definition["conform"]["lat"]
        lon_name = source_definition["conform"]["lon"]
        if lon_name in source_row:
            source_x = source_row[lon_name]
        else:
            source_x = source_row[lon_name.upper()]
        if lat_name in source_row:
            source_y = source_row[lat_name]
        else:
            source_y = source_row[lat_name.upper()]

    # Prepare an output row with the source lat and lon columns deleted
    out_row = copy.deepcopy(source_row)
    for n in lon_name, lon_name.upper(), lat_name, lat_name.upper():
        if n in out_row: del out_row[n]

    # Convert commas to periods for decimal numbers. (Not using locale.)
    try:
        source_x = source_x.replace(',', '.')
        source_y = source_y.replace(',', '.')
    except AttributeError:
        # Add blank data to the output CSV and get out
        out_row[X_FIELDNAME] = None
        out_row[Y_FIELDNAME] = None
        return out_row

    # Reproject the coordinates if necessary
    if "srs" not in source_definition["conform"]:
        out_x = source_x
        out_y = source_y
    else:
        try:
            srs = source_definition["conform"]["srs"]
            source_x = float(source_x)
            source_y = float(source_y)
            point = ogr.Geometry(ogr.wkbPoint)
            point.AddPoint_2D(float(source_x), float(source_y))

            point.Transform(_transform_to_4326(srs))
            out_x = "%.7f" % point.GetX()
            out_y = "%.7f" % point.GetY()
        except (TypeError, ValueError) as e:
            if not (source_x == "" or source_y == ""):
                _L.debug("Could not reproject %s %s in SRS %s", source_x, source_y, srs)
            out_x = ""
            out_y = ""

    # Add the reprojected data to the output CSV
    out_row[X_FIELDNAME] = out_x
    out_row[Y_FIELDNAME] = out_y
    return out_row

### Row-level conform code. Inputs and outputs are individual rows in a CSV file.
### The input row may or may not be modified in place. The output row is always returned.

def row_transform_and_convert(sd, row):
    "Apply the full conform transform and extract operations to a row"

    # Some conform specs have fields named with a case different from the source
    row = row_smash_case(sd, row)

    c = sd["conform"]
    
    "Attribute tags can utilize processing fxns"
    for k, v in c.items():
        if k in attrib_types and type(c[k]) is list:
            "Lists are a concat shortcut to concat fields with spaces"
            row = row_merge(sd, row, k)
        if k in attrib_types and type(c[k]) is dict:
            "Dicts are custom processing functions"
            if c[k]["function"] == "join":
                row = row_fxn_join(sd, row, k) 
            elif c[k]["function"] == "regexp":
                row = row_fxn_regexp(sd, row, k)
            elif c[k]["function"] == "format":
                row = row_fxn_format(sd, row, k)
            elif c[k]["function"] == "prefixed_number":
                row = row_fxn_prefixed_number(sd, row, k)
            elif c[k]["function"] == "postfixed_street":
                row = row_fxn_postfixed_street(sd, row, k)

    if "advanced_merge" in c:
        raise ValueError('Found unsupported "advanced_merge" option in conform')
    if "split" in c:
        raise ValueError('Found unsupported "split" option in conform')
    
    # Make up a random fingerprint if none exists
    cache_fingerprint = sd.get('fingerprint', str(uuid4()))
    
    row2 = row_convert_to_out(sd, row)
    row3 = row_canonicalize_unit_and_number(sd, row2)
    row4 = row_round_lat_lon(sd, row3)
    row5 = row_calculate_hash(cache_fingerprint, row4)
    return row5

def conform_smash_case(source_definition):
    "Convert all named fields in source_definition object to lowercase. Returns new object."
    new_sd = copy.deepcopy(source_definition)
    conform = new_sd["conform"]
    for k, v in conform.items():
        if v not in (X_FIELDNAME, Y_FIELDNAME) and getattr(v, 'lower', None):
            conform[k] = v.lower()
        if type(conform[k]) is list:
           conform[k] = [s.lower() for s in conform[k]] 
        if type(conform[k]) is dict:
            if "field" in conform[k]:
                conform[k]["field"] = conform[k]["field"].lower()
            elif "fields" in conform[k]:
                conform[k]["fields"] = [s.lower() for s in conform[k]["fields"]]
    
    if "advanced_merge" in conform:
        raise ValueError('Found unsupported "advanced_merge" option in conform')
    return new_sd

def row_smash_case(sd, input):
    "Convert all field names to lowercase. Slow, but necessary for imprecise conform specs."
    output = { k if k in (X_FIELDNAME, Y_FIELDNAME) else k.lower() : v for (k, v) in input.items() }
    return output

def row_merge(sd, row, key):
    "Merge multiple columns like 'Maple','St' to 'Maple St'"
    merge_data = [row[field] for field in sd["conform"][key]]
    row[attrib_types[key]] = ' '.join(merge_data)
    return row

def row_fxn_join(sd, row, key):
    "Create new columns by merging arbitrary other columns with a separator"
    fxn = sd["conform"][key]
    separator = fxn.get("separator", " ")
    try:
        fields = [(row[n] or u'').strip() for n in fxn["fields"]]
        row[attrib_types[key]] = separator.join([f for f in fields if f])
    except Exception as e:
        _L.debug("Failure to merge row %r %s", e, row)
    return row

def row_fxn_regexp(sd, row, key):
    "Split addresses like '123 Maple St' into '123' and 'Maple St'"
    fxn = sd["conform"][key]
    pattern = re.compile(fxn.get("pattern", False))
    replace = fxn.get('replace', False)
    if replace:
        match = re.sub(pattern, convert_regexp_replace(replace), row[fxn["field"]])
        row[attrib_types[key]] = match;
    else:
        match = pattern.search(row[fxn["field"]])
        row[attrib_types[key]] = ''.join(match.groups()) if match else '';
    return row

def row_fxn_prefixed_number(sd, row, key):
    "Extract '123' from '123 Maple St'"
    fxn = sd["conform"][key]

    match = prefixed_number_pattern.search(row[fxn["field"]])
    row[attrib_types[key]] = ''.join(match.groups()) if match else '';

    return row

def row_fxn_postfixed_street(sd, row, key):
    "Extract 'Maple St' from '123 Maple St'"
    fxn = sd["conform"][key]

    match = postfixed_street_pattern.search(row[fxn["field"]])
    row[attrib_types[key]] = ''.join(match.groups()) if match else '';

    return row

def row_fxn_format(sd, row, key):
    "Format multiple fields using a user-specified format string"
    fxn = sd["conform"][key]

    format_var_pattern = re.compile('\$([0-9]+)')

    fields = [(row[n] or u'').strip() for n in fxn["fields"]]

    parts = []

    idx = 0
    num_fields_added = 0

    format_str = fxn["format"]
    for i, m in enumerate(format_var_pattern.finditer(format_str)):
        field_idx = int(m.group(1))
        start, end = m.span()

        if field_idx > 0 and field_idx - 1 < len(fields):
            field = fields[field_idx - 1]

            if idx == 0 or (num_fields_added > 0 and field):
                parts.append(format_str[idx:start])

            if field:
                parts.append(field)
                num_fields_added += 1

        idx = end

    if num_fields_added > 0:
        parts.append(format_str[idx:])
        row[attrib_types[key]] = u''.join(parts)
    else:
        row[attrib_types[key]] = u''

    return row

def row_canonicalize_unit_and_number(sd, row):
    "Canonicalize address unit and number"
    row["UNIT"] = (row["UNIT"] or '').strip()
    row["NUMBER"] = (row["NUMBER"] or '').strip()
    if row["NUMBER"].endswith(".0"):
        row["NUMBER"] = row["NUMBER"][:-2]
    row["STREET"] = (row["STREET"] or '').strip()
    return row

def _round_wgs84_to_7(n):
    "Round a WGS84 coordinate to 7 decimal points. Input and output both strings."
    try:
        return "%.12g" % round(float(n), 7)
    except:
        return n
def row_round_lat_lon(sd, row):
    "Round WGS84 coordinates to 1cm precision"
    row["LON"] = _round_wgs84_to_7(row["LON"])
    row["LAT"] = _round_wgs84_to_7(row["LAT"])
    return row

def row_calculate_hash(cache_fingerprint, row):
    ''' Calculate row hash based on content and existing fingerprint.
    
        16 chars of SHA-1 gives a 64-bit value, plenty for all addresses.
    '''
    hash = sha1(cache_fingerprint.encode('utf8'))
    hash.update(json.dumps(sorted(row.items()), separators=(',', ':')).encode('utf8'))
    row.update(HASH=hash.hexdigest()[:16])
    
    return row

def row_convert_to_out(sd, row):
    "Convert a row from the source schema to OpenAddresses output schema"
    # note: sd["conform"]["lat"] and lon were already applied in the extraction from source
    
    keys = {}
    for k, v in attrib_types.items():
        if attrib_types[k] in row:
            keys[k] = attrib_types[k]
        else:
            keys[k] = sd['conform'].get(k, False)

    return {
        "LON": row.get(X_FIELDNAME, None),
        "LAT": row.get(Y_FIELDNAME, None),
        "UNIT": row.get(keys['unit'], None) if keys['unit'] else None,
        "NUMBER": row.get(keys['number'], None) if keys['number'] else None,
        "STREET": row.get(keys['street'], None) if keys['street'] else None,
        "CITY": row.get(keys['city'], None) if keys['city'] else None,
        "DISTRICT": row.get(keys['district'], None) if keys['district'] else None,
        "REGION": row.get(keys['region'], None) if keys['region'] else None,
        "POSTCODE": row.get(keys['postcode'], None) if keys['postcode'] else None,
        "ID": row.get(keys['id'], None) if keys['id'] else None,
    }

### File-level conform code. Inputs and outputs are filenames.

def extract_to_source_csv(source_definition, source_path, extract_path):
    """Extract arbitrary downloaded sources to an extracted CSV in the source schema.
    source_definition: description of the source, containing the conform object
    extract_path: file to write the extracted CSV file

    The extracted file will be in UTF-8 and will have X and Y columns corresponding
    to longitude and latitude in EPSG:4326.
    """
    if source_definition["conform"]["type"] in ("shapefile", "shapefile-polygon", "xml", "gdb"):
        ogr_source_path = normalize_ogr_filename_case(source_path)
        ogr_source_to_csv(source_definition, ogr_source_path, extract_path)
    elif source_definition["conform"]["type"] == "csv":
        csv_source_to_csv(source_definition, source_path, extract_path)
    elif source_definition["conform"]["type"] == "geojson":
        # GeoJSON sources have some awkward legacy with ESRI, see issue #34
        if source_definition["type"] == "ESRI":
            _L.info("ESRI GeoJSON source found; treating it as CSV")
            csv_source_to_csv(source_definition, source_path, extract_path)
        else:
            _L.info("Non-ESRI GeoJSON source found; converting as a stream.")
            geojson_source_path = normalize_ogr_filename_case(source_path)
            geojson_source_to_csv(geojson_source_path, extract_path)
    else:
        raise Exception("Unsupported source type %s" % source_definition["conform"]["type"])

def transform_to_out_csv(source_definition, extract_path, dest_path):
    ''' Transform an extracted source CSV to the OpenAddresses output CSV by applying conform rules.

        source_definition: description of the source, containing the conform object
        extract_path: extracted CSV file to process
        dest_path: path for output file in OpenAddress CSV
    '''
    # Convert all field names in the conform spec to lower case
    source_definition = conform_smash_case(source_definition)

    # Read through the extract CSV
    with csvopen(extract_path, 'r', encoding='utf-8') as extract_fp:
        reader = csvDictReader(extract_fp, encoding='utf-8')
        # Write to the destination CSV
        with csvopen(dest_path, 'w', encoding='utf-8') as dest_fp:
            writer = csvDictWriter(dest_fp, OPENADDR_CSV_SCHEMA, encoding='utf-8')
            writer.writeheader()
            # For every row in the extract
            for extract_row in reader:
                out_row = row_transform_and_convert(source_definition, extract_row)
                writer.writerow(out_row)

def conform_cli(source_definition, source_path, dest_path):
    "Command line entry point for conforming a downloaded source to an output CSV."
    # TODO: this tool only works if the source creates a single output

    if "conform" not in source_definition:
        return 1
    if not source_definition["conform"].get("type", None) in ["shapefile", "shapefile-polygon", "geojson", "csv", "xml", "gdb"]:
        _L.warning("Skipping file with unknown conform: %s", source_path)
        return 1

    # Create a temporary filename for the intermediate extracted source CSV
    fd, extract_path = tempfile.mkstemp(prefix='openaddr-extracted-', suffix='.csv')
    os.close(fd)
    _L.debug('extract temp file %s', extract_path)

    try:
        extract_to_source_csv(source_definition, source_path, extract_path)
        transform_to_out_csv(source_definition, extract_path, dest_path)
    finally:
        os.remove(extract_path)

    return 0

def conform_license(license):
    ''' Convert optional license tag.
    '''
    if license is None:
        return None
    
    if not hasattr(license, 'get'):
        # Old behavior: treat it like a string instead of a dictionary
        return license if hasattr(license, 'encode') else str(license)
    
    if 'url' in license and 'text' in license:
        return '{text} ({url})'.format(**license)
    elif 'url' in license:
        url = license['url']
        return url if hasattr(url, 'encode') else str(url)
    elif 'text' in license:
        text = license['text']
        return text if hasattr(text, 'encode') else str(text)
    else:
        return None
    
    raise ValueError('Unknown license format "{}"'.format(repr(license)))

def conform_attribution(license, attribution):
    ''' Convert optional license and attribution tags.
    
        Return tuple with attribution-required flag and attribution name.
    '''
    # Initially guess based on old attribution tag.
    if attribution in (None, False, ''):
        attr_flag = False
        attr_name = None
    elif not hasattr(attribution, 'encode'):
        attr_flag = True
        attr_name = str(attribution)
    else:
        attr_flag = True
        attr_name = attribution
    
    is_dict = license is not None and hasattr(license, 'get')
    
    # Look for an attribution name inside license dictionary
    if is_dict and 'attribution name' in license:
        if not hasattr(license['attribution name'], 'encode'):
            attr_flag = True
            attr_name = str(license['attribution name'])
        elif license['attribution name']:
            attr_flag = True
            attr_name = license['attribution name']
    
    # Look for an explicit flag inside license dictionary.
    if is_dict and 'attribution' in license:
        attr_flag = license['attribution']
    
    # Override null flag if name has been defined.
    if attr_flag is None and attr_name:
        attr_flag = True
    
    # Blank name if flag is not true.
    if not attr_flag:
        attr_name = None
    
    return attr_flag, attr_name

def conform_sharealike(license):
    ''' Convert optional license share-alike tags.
    
        Return boolean share-alike flag.
    '''
    is_dict = license is not None and hasattr(license, 'get')
    
    if not is_dict or 'share-alike' not in license:
        return None
    
    share_alike = license.get('share-alike')
    
    if share_alike is None:
        return False

    if share_alike is False:
        return False

    if share_alike is True:
        return True
    
    if hasattr(share_alike, 'lower'):
        if share_alike.lower() in ('n', 'no', 'f', 'false', ''):
            return False
    
    if hasattr(share_alike, 'lower'):
        if share_alike.lower() in ('y', 'yes', 't', 'true'):
            return True

