# coding=ascii

from __future__ import absolute_import, division, print_function
import logging; _L = logging.getLogger('openaddr.conform')

from .compat import standard_library

import os
import errno
import tempfile
import json
import copy
import sys

from zipfile import ZipFile
from argparse import ArgumentParser
from locale import getpreferredencoding

from .compat import csvopen, csvreader, csvDictReader, csvDictWriter
from .sample import sample_geojson
from .expand import expand_street_name

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
    def decompress(self, source_paths, workdir):
        output_files = []
        expand_path = os.path.join(workdir, 'unzipped')
        mkdirsp(expand_path)

        for source_path in source_paths:
            with ZipFile(source_path, 'r') as z:
                for name in z.namelist():
                    expanded_file_path = z.extract(name, expand_path)
                    _L.debug("Expanded file %s", expanded_file_path)
                    output_files.append(expanded_file_path)
        return output_files

class ExcerptDataTask(object):
    ''' Task for sampling three rows of data from datasource.
    '''
    known_types = ('.shp', '.json', '.csv', '.kml')

    def excerpt(self, source_paths, workdir, encoding):
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

        if not encoding:
            encoding = guess_source_encoding(datasource, layer)
        
        layer_defn = layer.GetLayerDefn()
        fieldcount = layer_defn.GetFieldCount()
        fieldnames = [layer_defn.GetFieldDefn(i).GetName() for i in range(fieldcount)]
        fieldnames = [f.decode(encoding) if hasattr(f, 'decode') else f for f in fieldnames]

        data_sample = [fieldnames]
        
        for feature in layer:
            row = [feature.GetField(i) for i in range(fieldcount)]
            row = [v.decode(encoding) if hasattr(v, 'decode') else v for v in row]
            data_sample.append(row)

            if len(data_sample) == 6:
                break
        
        # Determine geometry_type from layer, sample, or give up.
        if layer_defn.GetGeomType() in geometry_types:
            geometry_type = geometry_types.get(layer_defn.GetGeomType(), None)
        elif fieldnames[-3:] == ['X', 'Y', 'geom']:
            geometry = ogr.CreateGeometryFromWkt(data_sample[1][-1])
            geometry_type = geometry_types.get(geometry.GetGeometryType(), None)
        else:
            geometry_type = None

        return data_sample, geometry_type

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
    "Figure out which of the possible paths is the actual source"
    conform = source_definition["conform"]
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
            if ext.lower() == ".json":
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
        # We don't expect to be handed a list of files
        return source_paths[0]
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
        _L.warning("Unknown source type %s", conform["type"])
        return None

class ConvertToCsvTask(object):
    known_types = ('.shp', '.json', '.csv', '.kml')

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
                # Success! Return the path of the output CSV
                return dest_path

        # Conversion must have failed
        return None

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
        _L.warn("No encoding given and OGR couldn't guess. Trying ISO-8859-1, YOLO!")
        shp_encoding = "iso-8859-1"
    _L.debug("Assuming shapefile data is encoded %s", shp_encoding)

    # Get the input schema, create an output schema
    in_layer_defn = in_layer.GetLayerDefn()
    out_fieldnames = []
    for i in range(0, in_layer_defn.GetFieldCount()):
        field_defn = in_layer_defn.GetFieldDefn(i)
        out_fieldnames.append(field_defn.GetName())
    out_fieldnames.append('X')
    out_fieldnames.append('Y')

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
                    row['X'] = xmin/2 + xmax/2
                    row['Y'] = ymin/2 + ymax/2
                else:
                    row['X'] = centroid.GetX()
                    row['Y'] = centroid.GetY()
            else:
                row['X'] = None
                row['Y'] = None

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
                temp_reader = csvreader(source_fp, encoding=enc, delimiter=delim)
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
        # Remove headers for the old lat and lon fields (uppercase versions too)
        old_latlon = [source_definition["conform"]["lat"], source_definition["conform"]["lon"]]
        old_latlon.extend([s.upper() for s in old_latlon])
        out_fieldnames = [fn for fn in reader.fieldnames if fn not in old_latlon]
        # Append our special names X and Y
        out_fieldnames.append("X")
        out_fieldnames.append("Y")

        # Write the extracted CSV file
        with csvopen(dest_path, 'w', encoding='utf-8') as dest_fp:
            writer = csvDictWriter(dest_fp, out_fieldnames)
            writer.writeheader()
            # For every row in the source CSV
            for source_row in reader:
                if len(source_row) != num_fields:
                    _L.debug("Skipping row. Got %d columns, expected %d", len(source_row), num_fields)
                    continue
                out_row = row_extract_and_reproject(source_definition, source_row)
                writer.writerow(out_row)

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
    """Find lat/lon in source CSV data and store it in ESPG:4326 in X/Y in the row"""
    # Find the lat and lon in the source; work around case mismatch
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
    out_row["X"] = out_x
    out_row["Y"] = out_y
    return out_row

### Row-level conform code. Inputs and outputs are individual rows in a CSV file.
### The input row may or may not be modified in place. The output row is always returned.

def row_transform_and_convert(sd, row):
    "Apply the full conform transform and extract operations to a row"

    # Some conform specs have fields named with a case different from the source
    row = row_smash_case(sd, row)

    c = sd["conform"]
    if "merge" in c:
        row = row_merge_street(sd, row)
    if "advanced_merge" in c:
        row = row_advanced_merge(sd, row)
    if "split" in c:
        row = row_split_address(sd, row)
    row = row_convert_to_out(sd, row)
    row = row_canonicalize_street_and_number(sd, row)
    row = row_round_lat_lon(sd, row)
    return row

def conform_smash_case(source_definition):
    "Convert all named fields in source_definition object to lowercase. Returns new object."
    new_sd = copy.deepcopy(source_definition)
    conform = new_sd["conform"]
    for k in ("split", "lat", "lon", "street", "number"):
        if k in conform:
            conform[k] = conform[k].lower()
    if "merge" in conform:
        conform["merge"] = [s.lower() for s in conform["merge"]]
    if "advanced_merge" in conform:
        for new_col, spec in conform["advanced_merge"].items():
            spec["fields"] = [s.lower() for s in spec["fields"]]
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
    "Create new columns by merging arbitrary other columns with a separator"
    advanced_merge = sd["conform"]["advanced_merge"]
    for new_field_name, merge_spec in advanced_merge.items():
        separator = merge_spec.get("separator", " ")
        try:
            row[new_field_name] = separator.join([row[n] for n in merge_spec["fields"]])
        except Exception as e:
            _L.debug("Failure to merge row %r %s", e, row)
    return row

def row_split_address(sd, row):
    "Split addresses like '123 Maple St' into '123' and 'Maple St'"
    cols = row[sd["conform"]["split"]].split(' ', 1)  # maxsplit
    row['auto_number'] = cols[0]
    row['auto_street'] = cols[1] if len(cols) > 1 else ''
    return row

def row_canonicalize_street_and_number(sd, row):
    "Expand abbreviations and otherwise canonicalize street name and number"
    row["NUMBER"] = (row["NUMBER"] or '').strip()
    if row["NUMBER"].endswith(".0"):
        row["NUMBER"] = row["NUMBER"][:-2]
    row["STREET"] = expand_street_name(row["STREET"])
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

def row_convert_to_out(sd, row):
    "Convert a row from the source schema to OpenAddresses output schema"
    # note: sd["conform"]["lat"] and lon were already applied in the extraction from source
    return {
        "LON": row.get("x", None),
        "LAT": row.get("y", None),
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
    if source_definition["conform"]["type"] in ("shapefile", "shapefile-polygon", "xml"):
        ogr_source_to_csv(source_definition, source_path, extract_path)
    elif source_definition["conform"]["type"] == "csv":
        csv_source_to_csv(source_definition, source_path, extract_path)
    elif source_definition["conform"]["type"] == "geojson":
        # GeoJSON sources have some awkward legacy with ESRI, see issue #34
        if source_definition["type"] == "ESRI":
            _L.info("ESRI GeoJSON source found; treating it as CSV")
            csv_source_to_csv(source_definition, source_path, extract_path)
        else:
            _L.info("Non-ESRI GeoJSON source found; this code is not well tested.")
            ogr_source_to_csv(source_definition, source_path, extract_path)
    else:
        raise Exception("Unsupported source type %s" % source_definition["conform"]["type"])

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
    with csvopen(extract_path, 'r', encoding='utf-8') as extract_fp:
        reader = csvDictReader(extract_fp, encoding='utf-8')
        # Write to the destination CSV
        with csvopen(dest_path, 'w', encoding='utf-8') as dest_fp:
            writer = csvDictWriter(dest_fp, _openaddr_csv_schema, encoding='utf-8')
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
    if not source_definition["conform"].get("type", None) in ["shapefile", "shapefile-polygon", "geojson", "csv", "xml"]:
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

def main():
    "Main entry point for openaddr-pyconform command line tool. (See setup.py)"

    parser = ArgumentParser(description='Conform a downloaded source file.')
    parser.add_argument('source_json', help='Required source JSON file name.')
    parser.add_argument('source_path', help='Required pathname to the actual source data file')
    parser.add_argument('dest_path', help='Required pathname, output file written here.')
    parser.add_argument('-l', '--logfile', help='Optional log file name.')
    parser.add_argument('-v', '--verbose', help='Turn on verbose logging', action="store_true")
    args = parser.parse_args()

    from .jobs import setup_logger
    setup_logger(logfile = args.logfile, log_level = logging.DEBUG if args.verbose else logging.WARNING)

    with open(args.source_json) as file:
        source_definition = json.load(file)
    rc = conform_cli(source_definition, args.source_path, args.dest_path)
    return rc

if __name__ == '__main__':
    exit(main())


# Test suite. This code could be in a separate file

import unittest, tempfile, shutil

class TestConformTransforms (unittest.TestCase):
    "Test low level data transform functions"

    def test_row_smash_case(self):
        r = row_smash_case(None, {"UPPER": "foo", "lower": "bar", "miXeD": "mixed"})
        self.assertEqual({"upper": "foo", "lower": "bar", "mixed": "mixed"}, r)

    def test_conform_smash_case(self):
        d = { "conform": { "street": "MiXeD", "number": "U", "split": "U", "lat": "Y", "lon": "x",
                           "merge": [ "U", "l", "MiXeD" ],
                           "advanced_merge": { "auto_street": { "fields": ["MiXeD", "UPPER"] } } } }
        r = conform_smash_case(d)
        self.assertEqual({ "conform": { "street": "mixed", "number": "u", "split": "u", "lat": "y", "lon": "x",
                           "merge": [ "u", "l", "mixed" ],
                           "advanced_merge": { "auto_street": { "fields": ["mixed", "upper"] } } } },
                         r)

    def test_row_convert_to_out(self):
        d = { "conform": { "street": "s", "number": "n", "lon": "x", "lat": "y" } }
        r = row_convert_to_out(d, {"s": "MAPLE LN", "n": "123", "x": "-119.2", "y": "39.3"})
        self.assertEqual({"LON": "-119.2", "LAT": "39.3", "STREET": "MAPLE LN", "NUMBER": "123"}, r)

    def test_row_merge_street(self):
        d = { "conform": { "merge": [ "n", "t" ] } }
        r = row_merge_street(d, {"n": "MAPLE", "t": "ST", "x": "foo"})
        self.assertEqual({"auto_street": "MAPLE ST", "x": "foo", "t": "ST", "n": "MAPLE"}, r)

    def test_row_advanced_merge(self):
        c = { "conform": { "advanced_merge": {
                "new_a": { "fields": ["a1"] },
                "new_b": { "fields": ["b1", "b2"] },
                "new_c": { "separator": "-", "fields": ["c1", "c2"] } } } }
        d = { "a1": "va1", "b1": "vb1", "b2": "vb2", "c1": "vc1", "c2": "vc2" }
        r = row_advanced_merge(c, d)
        e = copy.deepcopy(d)
        e.update({ "new_a": "va1", "new_b": "vb1 vb2", "new_c": "vc1-vc2"})
        self.assertEqual(e, d)

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
        r = row_transform_and_convert(d, { "n": "123", "s1": "MAPLE", "s2": "ST", "X": "-119.2", "Y": "39.3" })
        self.assertEqual({"STREET": "Maple Street", "NUMBER": "123", "LON": "-119.2", "LAT": "39.3"}, r)

        d = { "conform": { "street": "auto_street", "number": "auto_number", "split": "s", "lon": "y", "lat": "x" } }
        r = row_transform_and_convert(d, { "s": "123 MAPLE ST", "X": "-119.2", "Y": "39.3" })
        self.assertEqual({"STREET": "Maple Street", "NUMBER": "123", "LON": "-119.2", "LAT": "39.3"}, r)

    def test_row_canonicalize_street_and_number(self):
        r = row_canonicalize_street_and_number({}, {"NUMBER": "324 ", "STREET": " OAK DR."})
        self.assertEqual("324", r["NUMBER"])
        self.assertEqual("Oak Drive", r["STREET"])

        # Tests for integer conversion
        for e, a in (("324", " 324.0  "),
                     ("", ""),
                     ("3240", "3240"),
                     ("INVALID", "INVALID"),
                     ("324.5", "324.5")):
            r = row_canonicalize_street_and_number({}, {"NUMBER": a, "STREET": ""})
            self.assertEqual(e, r["NUMBER"])

    def test_row_canonicalize_street_and_no_number(self):
        r = row_canonicalize_street_and_number({}, {"NUMBER": None, "STREET": " OAK DR."})
        self.assertEqual("", r["NUMBER"])
        self.assertEqual("Oak Drive", r["STREET"])

    def test_row_round_lat_lon(self):
        r = row_round_lat_lon({}, {"LON": "39.14285717777", "LAT": "-121.20"})
        self.assertEqual({"LON": "39.1428572", "LAT": "-121.2"}, r)
        for e, a in ((    ""        ,    ""),
                     (  "39.3"      ,  "39.3"),
                     (  "39.3"      ,  "39.3000000"),
                     ( "-39.3"      , "-39.3000"),
                     (  "39.1428571",  "39.142857143"),
                     ( "139.1428572", "139.142857153"),
                     (  "39.1428572",  "39.142857153"),
                     (   "3.1428572",   "3.142857153"),
                     (   "0.1428572",   "0.142857153"),
                     ("-139.1428572","-139.142857153"),
                     ( "-39.1428572", "-39.142857153"),
                     (  "-3.1428572",  "-3.142857153"),
                     (  "-0.1428572",  "-0.142857153"),
                     (  "39.1428572",  "39.142857153"),
                     (   "0"        ,  " 0.00"),
                     (  "-0"        ,  "-0.00"),
                     ( "180"        ,  "180.0"),
                     ("-180"        , "-180")):
            r = row_round_lat_lon({}, {"LAT": a, "LON": a})
            self.assertEqual(e, r["LON"])

    def test_row_extract_and_reproject(self):
        d = { "conform" : { "lon": "longitude", "lat": "latitude" } }
        r = row_extract_and_reproject(d, {"longitude": "-122.3", "latitude": "39.1"})
        self.assertEqual({"Y": "39.1", "X": "-122.3"}, r)

        d = { "conform" : { "lon": "x", "lat": "y" } }
        r = row_extract_and_reproject(d, {"X": "-122.3", "Y": "39.1" })
        self.assertEqual({"X": "-122.3", "Y": "39.1"}, r)

        d = { "conform" : { "lon": "X", "lat": "Y", "srs": "EPSG:2913" } }
        r = row_extract_and_reproject(d, {"X": "7655634.924", "Y": "668868.414"})
        self.assertAlmostEqual(-122.630842186650796, float(r["X"]))
        self.assertAlmostEqual(45.481554393851063, float(r["Y"]))

        d = { "conform" : { "lon": "X", "lat": "Y", "srs": "EPSG:2913" } }
        r = row_extract_and_reproject(d, {"X": "", "Y": ""})
        self.assertEqual("", r["X"])
        self.assertEqual("", r["Y"])

class TestConformCli (unittest.TestCase):
    "Test the command line interface creates valid output files from test input"
    def setUp(self):
        self.testdir = tempfile.mkdtemp(prefix='openaddr-testPyConformCli-')
        self.conforms_dir = os.path.join(os.path.dirname(__file__), '..', 'tests', 'conforms')

    def tearDown(self):
        shutil.rmtree(self.testdir)

    def _run_conform_on_source(self, source_name, ext):
        "Helper method to run a conform on the named source. Assumes naming convention."
        with open(os.path.join(self.conforms_dir, "%s.json" % source_name)) as file:
            source_definition = json.load(file)
        source_path = os.path.join(self.conforms_dir, "%s.%s" % (source_name, ext))
        dest_path = os.path.join(self.testdir, '%s-conformed.csv' % source_name)

        rc = conform_cli(source_definition, source_path, dest_path)
        return rc, dest_path

    def test_unknown_conform(self):
        # Test that the conform tool does something reasonable with unknown conform sources
        self.assertEqual(1, conform_cli({}, 'test', ''))
        self.assertEqual(1, conform_cli({'conform': {}}, 'test', ''))
        self.assertEqual(1, conform_cli({'conform': {'type': 'broken'}}, 'test', ''))

    def test_lake_man(self):
        rc, dest_path = self._run_conform_on_source('lake-man', 'shp')
        self.assertEqual(0, rc)

        with csvopen(dest_path) as fp:
            reader = csvDictReader(fp)
            self.assertEqual(['LON', 'LAT', 'NUMBER', 'STREET'], reader.fieldnames)

            rows = list(reader)

            self.assertAlmostEqual(float(rows[0]['LAT']), 37.802612637607439)
            self.assertAlmostEqual(float(rows[0]['LON']), -122.259249687194824)

            self.assertEqual(6, len(rows))
            self.assertEqual(rows[0]['NUMBER'], '5115')
            self.assertEqual(rows[0]['STREET'], 'Fruited Plains Lane')
            self.assertEqual(rows[1]['NUMBER'], '5121')
            self.assertEqual(rows[1]['STREET'], 'Fruited Plains Lane')
            self.assertEqual(rows[2]['NUMBER'], '5133')
            self.assertEqual(rows[2]['STREET'], 'Fruited Plains Lane')
            self.assertEqual(rows[3]['NUMBER'], '5126')
            self.assertEqual(rows[3]['STREET'], 'Fruited Plains Lane')
            self.assertEqual(rows[4]['NUMBER'], '5120')
            self.assertEqual(rows[4]['STREET'], 'Fruited Plains Lane')
            self.assertEqual(rows[5]['NUMBER'], '5115')
            self.assertEqual(rows[5]['STREET'], 'Old Mill Road')

    def test_lake_man_split(self):
        rc, dest_path = self._run_conform_on_source('lake-man-split', 'shp')
        self.assertEqual(0, rc)
        
        with csvopen(dest_path) as fp:
            rows = list(csvDictReader(fp))
            self.assertEqual(rows[0]['NUMBER'], '915')
            self.assertEqual(rows[0]['STREET'], 'Edward Avenue')
            self.assertEqual(rows[1]['NUMBER'], '3273')
            self.assertEqual(rows[1]['STREET'], 'Peter Street')
            self.assertEqual(rows[2]['NUMBER'], '976')
            self.assertEqual(rows[2]['STREET'], 'Ford Boulevard')
            self.assertEqual(rows[3]['NUMBER'], '7055')
            self.assertEqual(rows[3]['STREET'], 'Saint Rose Avenue')
            self.assertEqual(rows[4]['NUMBER'], '534')
            self.assertEqual(rows[4]['STREET'], 'Wallace Avenue')
            self.assertEqual(rows[5]['NUMBER'], '531')
            self.assertEqual(rows[5]['STREET'], 'Scofield Avenue')

    def test_lake_man_merge_postcode(self):
        rc, dest_path = self._run_conform_on_source('lake-man-merge-postcode', 'shp')
        self.assertEqual(0, rc)
        
        with csvopen(dest_path) as fp:
            rows = list(csvDictReader(fp))
            self.assertEqual(rows[0]['NUMBER'], '35845')
            self.assertEqual(rows[0]['STREET'], 'Eklutna Lake Road')
            self.assertEqual(rows[1]['NUMBER'], '35850')
            self.assertEqual(rows[1]['STREET'], 'Eklutna Lake Road')
            self.assertEqual(rows[2]['NUMBER'], '35900')
            self.assertEqual(rows[2]['STREET'], 'Eklutna Lake Road')
            self.assertEqual(rows[3]['NUMBER'], '35870')
            self.assertEqual(rows[3]['STREET'], 'Eklutna Lake Road')
            self.assertEqual(rows[4]['NUMBER'], '32551')
            self.assertEqual(rows[4]['STREET'], 'Eklutna Lake Road')
            self.assertEqual(rows[5]['NUMBER'], '31401')
            self.assertEqual(rows[5]['STREET'], 'Eklutna Lake Road')
    
    def test_lake_man_merge_postcode2(self):
        rc, dest_path = self._run_conform_on_source('lake-man-merge-postcode2', 'shp')
        self.assertEqual(0, rc)
        
        with csvopen(dest_path) as fp:
            rows = list(csvDictReader(fp))
            self.assertEqual(rows[0]['NUMBER'], '85')
            self.assertEqual(rows[0]['STREET'], 'Maitland Drive')
            self.assertEqual(rows[1]['NUMBER'], '81')
            self.assertEqual(rows[1]['STREET'], 'Maitland Drive')
            self.assertEqual(rows[2]['NUMBER'], '92')
            self.assertEqual(rows[2]['STREET'], 'Maitland Drive')
            self.assertEqual(rows[3]['NUMBER'], '92')
            self.assertEqual(rows[3]['STREET'], 'Maitland Drive')
            self.assertEqual(rows[4]['NUMBER'], '92')
            self.assertEqual(rows[4]['STREET'], 'Maitland Drive')
            self.assertEqual(rows[5]['NUMBER'], '92')
            self.assertEqual(rows[5]['STREET'], 'Maitland Drive')

    def test_lake_man_shp_utf8(self):
        rc, dest_path = self._run_conform_on_source('lake-man-utf8', 'shp')
        self.assertEqual(0, rc)
        with csvopen(dest_path, encoding='utf-8') as fp:
            rows = list(csvDictReader(fp, encoding='utf-8'))
            self.assertEqual(rows[0]['STREET'], u'Pz Espa\u00f1a')

    def test_lake_man_shp_epsg26943(self):
        rc, dest_path = self._run_conform_on_source('lake-man-epsg26943', 'shp')
        self.assertEqual(0, rc)

        with csvopen(dest_path) as fp:
            rows = list(csvDictReader(fp))
            self.assertAlmostEqual(float(rows[0]['LAT']), 37.802612637607439)
            self.assertAlmostEqual(float(rows[0]['LON']), -122.259249687194824)

    def test_lake_man_shp_noprj_epsg26943(self):
        rc, dest_path = self._run_conform_on_source('lake-man-epsg26943-noprj', 'shp')
        self.assertEqual(0, rc)

        with csvopen(dest_path) as fp:
            rows = list(csvDictReader(fp))
            self.assertAlmostEqual(float(rows[0]['LAT']), 37.802612637607439)
            self.assertAlmostEqual(float(rows[0]['LON']), -122.259249687194824)

    # TODO: add tests for non-ESRI GeoJSON sources

    def test_lake_man_split2(self):
        "An ESRI-to-CSV like source"
        rc, dest_path = self._run_conform_on_source('lake-man-split2', 'csv')
        self.assertEqual(0, rc)

        with csvopen(dest_path) as fp:
            rows = list(csvDictReader(fp))
            self.assertEqual(rows[0]['NUMBER'], '1')
            self.assertEqual(rows[0]['STREET'], 'Spectrum Pointe Drive #320')
            self.assertEqual(rows[1]['NUMBER'], '')
            self.assertEqual(rows[1]['STREET'], '')
            self.assertEqual(rows[2]['NUMBER'], '300')
            self.assertEqual(rows[2]['STREET'], 'East Chapman Avenue')
            self.assertEqual(rows[3]['NUMBER'], '1')
            self.assertEqual(rows[3]['STREET'], 'Spectrum Pointe Drive #320')
            self.assertEqual(rows[4]['NUMBER'], '1')
            self.assertEqual(rows[4]['STREET'], 'Spectrum Pointe Drive #320')
            self.assertEqual(rows[5]['NUMBER'], '1')
            self.assertEqual(rows[5]['STREET'], 'Spectrum Pointe Drive #320')

    def test_nara_jp(self):
        "Test case from jp-nara.json"
        rc, dest_path = self._run_conform_on_source('jp-nara', 'csv')
        self.assertEqual(0, rc)
        with csvopen(dest_path) as fp:
            rows = list(csvDictReader(fp))
            self.assertEqual(rows[0]['NUMBER'], '2543-6')
            self.assertAlmostEqual(float(rows[0]['LON']), 135.955104)
            self.assertAlmostEqual(float(rows[0]['LAT']), 34.607832)
            self.assertEqual(rows[0]['STREET'], u'\u91dd\u753a')
            self.assertEqual(rows[1]['NUMBER'], '202-6')

    def test_lake_man_3740(self):
        "CSV in an oddball SRS"
        rc, dest_path = self._run_conform_on_source('lake-man-3740', 'csv')
        self.assertEqual(0, rc)
        with csvopen(dest_path) as fp:
            rows = list(csvDictReader(fp))
            self.assertAlmostEqual(float(rows[0]['LAT']), 37.802612637607439, places=5)
            self.assertAlmostEqual(float(rows[0]['LON']), -122.259249687194824, places=5)
            self.assertEqual(rows[0]['NUMBER'], '5')
            self.assertEqual(rows[0]['STREET'], u'Pz Espa\u00f1a')

    def test_lake_man_gml(self):
        "GML XML files"
        rc, dest_path = self._run_conform_on_source('lake-man-gml', 'gml')
        self.assertEqual(0, rc)
        with csvopen(dest_path) as fp:
            rows = list(csvDictReader(fp))
            self.assertEqual(6, len(rows))
            self.assertAlmostEqual(float(rows[0]['LAT']), 37.802612637607439)
            self.assertAlmostEqual(float(rows[0]['LON']), -122.259249687194824)
            self.assertEqual(rows[0]['NUMBER'], '5115')
            self.assertEqual(rows[0]['STREET'], 'Fruited Plains Lane')


class TestConformMisc(unittest.TestCase):
    def test_find_shapefile_source_path(self):
        shp_conform = {"conform": { "type": "shapefile" } }
        self.assertEqual("foo.shp", find_source_path(shp_conform, ["foo.shp"]))
        self.assertEqual("FOO.SHP", find_source_path(shp_conform, ["FOO.SHP"]))
        self.assertEqual("xyzzy/FOO.SHP", find_source_path(shp_conform, ["xyzzy/FOO.SHP"]))
        self.assertEqual("foo.shp", find_source_path(shp_conform, ["foo.shp", "foo.prj", "foo.shx"]))
        self.assertEqual(None, find_source_path(shp_conform, ["nope.txt"]))
        self.assertEqual(None, find_source_path(shp_conform, ["foo.shp", "bar.shp"]))

        shp_file_conform = {"conform": { "type": "shapefile", "file": "foo.shp" } }
        self.assertEqual("foo.shp", find_source_path(shp_file_conform, ["foo.shp"]))
        self.assertEqual("foo.shp", find_source_path(shp_file_conform, ["foo.shp", "bar.shp"]))
        self.assertEqual("xyzzy/foo.shp", find_source_path(shp_file_conform, ["xyzzy/foo.shp", "xyzzy/bar.shp"]))

        shp_poly_conform = {"conform": { "type": "shapefile-polygon" } }
        self.assertEqual("foo.shp", find_source_path(shp_poly_conform, ["foo.shp"]))

        broken_conform = {"conform": { "type": "broken" }}
        self.assertEqual(None, find_source_path(broken_conform, ["foo.shp"]))

    def test_find_geojson_source_path(self):
        geojson_conform = {"type": "notESRI", "conform": {"type": "geojson"}}
        self.assertEqual("foo.json", find_source_path(geojson_conform, ["foo.json"]))
        self.assertEqual("FOO.JSON", find_source_path(geojson_conform, ["FOO.JSON"]))
        self.assertEqual("xyzzy/FOO.JSON", find_source_path(geojson_conform, ["xyzzy/FOO.JSON"]))
        self.assertEqual("foo.json", find_source_path(geojson_conform, ["foo.json", "foo.prj", "foo.shx"]))
        self.assertEqual(None, find_source_path(geojson_conform, ["nope.txt"]))
        self.assertEqual(None, find_source_path(geojson_conform, ["foo.json", "bar.json"]))

    def test_find_esri_source_path(self):
        # test that the legacy ESRI/GeoJSON style works
        old_conform = {"type": "ESRI", "conform": {"type": "geojson"}}
        self.assertEqual("foo.csv", find_source_path(old_conform, ["foo.csv"]))
        # test that the new ESRI/CSV style works
        new_conform = {"type": "ESRI", "conform": {"type": "csv"}}
        self.assertEqual("foo.csv", find_source_path(new_conform, ["foo.csv"]))

    def test_find_csv_source_path(self):
        csv_conform = {"conform": {"type": "csv"}}
        self.assertEqual("foo.csv", find_source_path(csv_conform, ["foo.csv"]))

    def test_find_xml_source_path(self):
        c = {"conform": {"type": "xml"}}
        self.assertEqual("foo.gml", find_source_path(c, ["foo.gml"]))
        c = {"conform": {"type": "xml", "file": "xyzzy/foo.gml"}}
        self.assertEqual("xyzzy/foo.gml", find_source_path(c, ["xyzzy/foo.gml", "bar.gml", "foo.gml"]))
        self.assertEqual("/tmp/foo/xyzzy/foo.gml", find_source_path(c, ["/tmp/foo/xyzzy/foo.gml"]))

class TestConformCsv(unittest.TestCase):
    "Fixture to create real files to test csv_source_to_csv()"

    # Test strings. an ASCII CSV file (with 1 row) and a Unicode CSV file,
    # along with expected outputs. These are Unicode strings; test code needs
    # to convert the input to bytes with the tested encoding.
    _ascii_header_in = u'STREETNAME,NUMBER,LATITUDE,LONGITUDE'
    _ascii_row_in = u'MAPLE ST,123,39.3,-121.2'
    _ascii_header_out = u'STREETNAME,NUMBER,X,Y'
    _ascii_row_out = u'MAPLE ST,123,-121.2,39.3'
    _unicode_header_in = u'STRE\u00c9TNAME,NUMBER,\u7def\u5ea6,LONGITUDE'
    _unicode_row_in = u'\u2603 ST,123,39.3,-121.2'
    _unicode_header_out = u'STRE\u00c9TNAME,NUMBER,X,Y'
    _unicode_row_out = u'\u2603 ST,123,-121.2,39.3'

    def setUp(self):
        self.testdir = tempfile.mkdtemp(prefix='openaddr-testPyConformCsv-')

    def tearDown(self):
        shutil.rmtree(self.testdir)

    def _convert(self, conform, src_bytes):
        "Convert a CSV source (list of byte strings) and return output as a list of unicode strings"
        self.assertNotEqual(type(src_bytes), type(u''))
        src_path = os.path.join(self.testdir, "input.csv")
        
        with open(src_path, "w+b") as file:
            file.write(b'\n'.join(src_bytes))

        dest_path = os.path.join(self.testdir, "output.csv")
        csv_source_to_csv(conform, src_path, dest_path)
        
        with open(dest_path, 'rb') as file:
            return [s.decode('utf-8').strip() for s in file]

    def test_simple(self):
        c = { "conform": { "type": "csv", "lat": "LATITUDE", "lon": "LONGITUDE" } }
        d = (self._ascii_header_in.encode('ascii'),
             self._ascii_row_in.encode('ascii'))
        r = self._convert(c, d)
        self.assertEqual(self._ascii_header_out, r[0])
        self.assertEqual(self._ascii_row_out, r[1])

    def test_utf8(self):
        c = { "conform": { "type": "csv", "lat": u"\u7def\u5ea6", "lon": u"LONGITUDE" } }
        d = (self._unicode_header_in.encode('utf-8'),
             self._unicode_row_in.encode('utf-8'))
        r = self._convert(c, d)
        self.assertEqual(self._unicode_header_out, r[0])
        self.assertEqual(self._unicode_row_out, r[1])

    def test_csvsplit(self):
        c = { "conform": { "csvsplit": ";", "type": "csv", "lat": "LATITUDE", "lon": "LONGITUDE" } }
        d = (self._ascii_header_in.replace(',', ';').encode('ascii'),
             self._ascii_row_in.replace(',', ';').encode('ascii'))
        r = self._convert(c, d)
        self.assertEqual(self._ascii_header_out, r[0])
        self.assertEqual(self._ascii_row_out, r[1])

        # unicodecsv freaked out about unicode strings for delimiter
        unicode_conform = { "conform": { "csvsplit": u";", "type": "csv", "lat": "LATITUDE", "lon": "LONGITUDE" } }
        r = self._convert(unicode_conform, d)
        self.assertEqual(self._ascii_row_out, r[1])

    def test_csvencoded_utf8(self):
        c = { "conform": { "encoding": "utf-8", "type": "csv", "lat": u"\u7def\u5ea6", "lon": u"LONGITUDE" } }
        d = (self._unicode_header_in.encode('utf-8'),
             self._unicode_row_in.encode('utf-8'))
        r = self._convert(c, d)
        self.assertEqual(self._unicode_header_out, r[0])
        self.assertEqual(self._unicode_row_out, r[1])

    def test_csvencoded_shift_jis(self):
        c = { "conform": { "encoding": "shift-jis", "type": "csv", "lat": u"\u7def\u5ea6", "lon": u"LONGITUDE" } }
        d = (u'\u5927\u5b57\u30fb\u753a\u4e01\u76ee\u540d,NUMBER,\u7def\u5ea6,LONGITUDE'.encode('shift-jis'),
             u'\u6771 ST,123,39.3,-121.2'.encode('shift-jis'))
        r = self._convert(c, d)
        self.assertEqual(u'\u5927\u5b57\u30fb\u753a\u4e01\u76ee\u540d,NUMBER,X,Y', r[0])
        self.assertEqual(u'\u6771 ST,123,-121.2,39.3', r[1])

    def test_headers_minus_one(self):
        c = { "conform": { "headers": -1, "type": "csv", "lon": "COLUMN4", "lat": "COLUMN3" } }
        d = (u'MAPLE ST,123,39.3,-121.2'.encode('ascii'),)
        r = self._convert(c, d)
        self.assertEqual(u'COLUMN1,COLUMN2,X,Y', r[0])
        self.assertEqual(u'MAPLE ST,123,-121.2,39.3', r[1])

    def test_headers_and_skiplines(self):
        c = {"conform": { "headers": 2, "skiplines": 2, "type": "csv", "lon": "LONGITUDE", "lat": "LATITUDE" } }
        d = (u'HAHA,THIS,HEADER,IS,FAKE'.encode('ascii'),
             self._ascii_header_in.encode('ascii'),
             self._ascii_row_in.encode('ascii')) 
        r = self._convert(c, d)
        self.assertEqual(self._ascii_header_out, r[0])
        self.assertEqual(self._ascii_row_out, r[1])

    def test_perverse_header_name_and_case(self):
        # This is an example inspired by the hipsters in us-or-portland
        # Conform says lowercase but the actual header is uppercase.
        # Also the columns are named X and Y in the input
        c = {"conform": {"lon": "x", "lat": "y", "number": "n", "street": "s", "type": "csv"}}
        d = (u'n,s,X,Y'.encode('ascii'),
             u'3203,SE WOODSTOCK BLVD,-122.629314,45.479425'.encode('ascii'))
        r = self._convert(c, d)
        self.assertEqual(u'n,s,X,Y', r[0])
        self.assertEqual(u'3203,SE WOODSTOCK BLVD,-122.629314,45.479425', r[1])

    def test_srs(self):
        # This is an example inspired by the hipsters in us-or-portland
        c = {"conform": {"lon": "x", "lat": "y", "srs": "EPSG:2913", "number": "n", "street": "s", "type": "csv"}}
        d = (u'n,s,X,Y'.encode('ascii'),
             u'3203,SE WOODSTOCK BLVD,7655634.924,668868.414'.encode('ascii'))
        r = self._convert(c, d)
        self.assertEqual(u'n,s,X,Y', r[0])
        self.assertEqual(u'3203,SE WOODSTOCK BLVD,-122.6308422,45.4815544', r[1])

    def test_too_many_columns(self):
        "Check that we don't barf on input with too many columns in some rows"
        c = { "conform": { "type": "csv", "lat": "LATITUDE", "lon": "LONGITUDE" } }
        d = (self._ascii_header_in.encode('ascii'),
             self._ascii_row_in.encode('ascii'),
             u'MAPLE ST,123,39.3,-121.2,EXTRY'.encode('ascii'))
        r = self._convert(c, d)
        self.assertEqual(2, len(r))
        self.assertEqual(self._ascii_header_out, r[0])
        self.assertEqual(self._ascii_row_out, r[1])
