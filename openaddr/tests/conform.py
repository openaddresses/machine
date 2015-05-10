# coding=ascii

from __future__ import absolute_import, division, print_function

import os
import copy
import json

import unittest
import tempfile
import shutil

from ..conform import (
    GEOM_FIELDNAME, X_FIELDNAME, Y_FIELDNAME,
    csv_source_to_csv, find_source_path, row_transform_and_convert,
    row_split_address, row_smash_case, row_round_lat_lon, row_merge_street,
    row_extract_and_reproject, row_convert_to_out, row_advanced_merge,
    row_canonicalize_street_and_number, conform_smash_case, conform_cli,
    csvopen, csvDictReader
    )

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
        d = { "conform": { "street": "s", "number": "n" } }
        r = row_convert_to_out(d, {"s": "MAPLE LN", "n": "123", X_FIELDNAME: "-119.2", Y_FIELDNAME: "39.3"})
        self.assertEqual({"LON": "-119.2", "LAT": "39.3", "STREET": "MAPLE LN", "NUMBER": "123", "POSTCODE": None}, r)

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
        r = row_transform_and_convert(d, { "n": "123", "s1": "MAPLE", "s2": "ST", X_FIELDNAME: "-119.2", Y_FIELDNAME: "39.3" })
        self.assertEqual({"STREET": "Maple Street", "NUMBER": "123", "LON": "-119.2", "LAT": "39.3", 'POSTCODE': None}, r)

        d = { "conform": { "street": "auto_street", "number": "auto_number", "split": "s", "lon": "y", "lat": "x" } }
        r = row_transform_and_convert(d, { "s": "123 MAPLE ST", X_FIELDNAME: "-119.2", Y_FIELDNAME: "39.3" })
        self.assertEqual({"STREET": "Maple Street", "NUMBER": "123", "LON": "-119.2", "LAT": "39.3", 'POSTCODE': None}, r)

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
        # CSV lat/lon column names
        d = { "conform" : { "lon": "longitude", "lat": "latitude", "type": "csv" }, 'type': 'test' }
        r = row_extract_and_reproject(d, {"longitude": "-122.3", "latitude": "39.1"})
        self.assertEqual({Y_FIELDNAME: "39.1", X_FIELDNAME: "-122.3"}, r)

        # non-CSV lat/lon column names
        d = { "conform" : { "lon": "x", "lat": "y", "type": "" }, 'type': 'test' }
        r = row_extract_and_reproject(d, {X_FIELDNAME: "-122.3", Y_FIELDNAME: "39.1" })
        self.assertEqual({X_FIELDNAME: "-122.3", Y_FIELDNAME: "39.1"}, r)

        # reprojection
        d = { "conform" : { "srs": "EPSG:2913", "type": "" }, 'type': 'test' }
        r = row_extract_and_reproject(d, {X_FIELDNAME: "7655634.924", Y_FIELDNAME: "668868.414"})
        self.assertAlmostEqual(-122.630842186650796, float(r[X_FIELDNAME]))
        self.assertAlmostEqual(45.481554393851063, float(r[Y_FIELDNAME]))

        d = { "conform" : { "lon": "X", "lat": "Y", "srs": "EPSG:2913", "type": "" }, 'type': 'test' }
        r = row_extract_and_reproject(d, {X_FIELDNAME: "", Y_FIELDNAME: ""})
        self.assertEqual("", r[X_FIELDNAME])
        self.assertEqual("", r[Y_FIELDNAME])

        # commas in lat/lon columns (eg Iceland)
        d = { "conform" : { "lon": "LONG_WGS84", "lat": "LAT_WGS84", "type": "csv" }, 'type': 'test' }
        r = row_extract_and_reproject(d, {"LONG_WGS84": "-21,77", "LAT_WGS84": "64,11"})
        self.assertEqual({Y_FIELDNAME: "64.11", X_FIELDNAME: "-21.77"}, r)

class TestConformCli (unittest.TestCase):
    "Test the command line interface creates valid output files from test input"
    def setUp(self):
        self.testdir = tempfile.mkdtemp(prefix='openaddr-testPyConformCli-')
        self.conforms_dir = os.path.join(os.path.dirname(__file__), 'conforms')

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
            self.assertEqual(['LON', 'LAT', 'NUMBER', 'STREET', 'POSTCODE'], reader.fieldnames)

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
        csv_file_conform = {"conform": {"type": "csv", "file":"bar.txt"}}
        self.assertEqual("bar.txt", find_source_path(csv_file_conform, ["license.pdf", "bar.txt"]))
        self.assertEqual("aa/bar.txt", find_source_path(csv_file_conform, ["license.pdf", "aa/bar.txt"]))
        self.assertEqual(None, find_source_path(csv_file_conform, ["foo.txt"]))

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
    _ascii_header_out = u'STREETNAME,NUMBER,{X_FIELDNAME},{Y_FIELDNAME}'.format(**globals())
    _ascii_row_out = u'MAPLE ST,123,-121.2,39.3'
    _unicode_header_in = u'STRE\u00c9TNAME,NUMBER,\u7def\u5ea6,LONGITUDE'
    _unicode_row_in = u'\u2603 ST,123,39.3,-121.2'
    _unicode_header_out = u'STRE\u00c9TNAME,NUMBER,{X_FIELDNAME},{Y_FIELDNAME}'.format(**globals())
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
        c = { "conform": { "type": "csv", "lat": "LATITUDE", "lon": "LONGITUDE" }, 'type': 'test' }
        d = (self._ascii_header_in.encode('ascii'),
             self._ascii_row_in.encode('ascii'))
        r = self._convert(c, d)
        self.assertEqual(self._ascii_header_out, r[0])
        self.assertEqual(self._ascii_row_out, r[1])

    def test_utf8(self):
        c = { "conform": { "type": "csv", "lat": u"\u7def\u5ea6", "lon": u"LONGITUDE" }, 'type': 'test' }
        d = (self._unicode_header_in.encode('utf-8'),
             self._unicode_row_in.encode('utf-8'))
        r = self._convert(c, d)
        self.assertEqual(self._unicode_header_out, r[0])
        self.assertEqual(self._unicode_row_out, r[1])

    def test_csvsplit(self):
        c = { "conform": { "csvsplit": ";", "type": "csv", "lat": "LATITUDE", "lon": "LONGITUDE" }, 'type': 'test' }
        d = (self._ascii_header_in.replace(',', ';').encode('ascii'),
             self._ascii_row_in.replace(',', ';').encode('ascii'))
        r = self._convert(c, d)
        self.assertEqual(self._ascii_header_out, r[0])
        self.assertEqual(self._ascii_row_out, r[1])

        # unicodecsv freaked out about unicode strings for delimiter
        unicode_conform = { "conform": { "csvsplit": u";", "type": "csv", "lat": "LATITUDE", "lon": "LONGITUDE" }, 'type': 'test' }
        r = self._convert(unicode_conform, d)
        self.assertEqual(self._ascii_row_out, r[1])

    def test_csvencoded_utf8(self):
        c = { "conform": { "encoding": "utf-8", "type": "csv", "lat": u"\u7def\u5ea6", "lon": u"LONGITUDE" }, 'type': 'test' }
        d = (self._unicode_header_in.encode('utf-8'),
             self._unicode_row_in.encode('utf-8'))
        r = self._convert(c, d)
        self.assertEqual(self._unicode_header_out, r[0])
        self.assertEqual(self._unicode_row_out, r[1])

    def test_csvencoded_shift_jis(self):
        c = { "conform": { "encoding": "shift-jis", "type": "csv", "lat": u"\u7def\u5ea6", "lon": u"LONGITUDE" }, 'type': 'test' }
        d = (u'\u5927\u5b57\u30fb\u753a\u4e01\u76ee\u540d,NUMBER,\u7def\u5ea6,LONGITUDE'.encode('shift-jis'),
             u'\u6771 ST,123,39.3,-121.2'.encode('shift-jis'))
        r = self._convert(c, d)
        self.assertEqual(r[0], u'\u5927\u5b57\u30fb\u753a\u4e01\u76ee\u540d,NUMBER,{X_FIELDNAME},{Y_FIELDNAME}'.format(**globals()))
        self.assertEqual(r[1], u'\u6771 ST,123,-121.2,39.3')

    def test_headers_minus_one(self):
        c = { "conform": { "headers": -1, "type": "csv", "lon": "COLUMN4", "lat": "COLUMN3" }, 'type': 'test' }
        d = (u'MAPLE ST,123,39.3,-121.2'.encode('ascii'),)
        r = self._convert(c, d)
        self.assertEqual(r[0], u'COLUMN1,COLUMN2,{X_FIELDNAME},{Y_FIELDNAME}'.format(**globals()))
        self.assertEqual(r[1], u'MAPLE ST,123,-121.2,39.3')

    def test_headers_and_skiplines(self):
        c = {"conform": { "headers": 2, "skiplines": 2, "type": "csv", "lon": "LONGITUDE", "lat": "LATITUDE" }, 'type': 'test' }
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
        c = {"conform": {"lon": "x", "lat": "y", "number": "n", "street": "s", "type": "csv"}, 'type': 'test'}
        d = (u'n,s,X,Y'.encode('ascii'),
             u'3203,SE WOODSTOCK BLVD,-122.629314,45.479425'.encode('ascii'))
        r = self._convert(c, d)
        self.assertEqual(r[0], u'n,s,{X_FIELDNAME},{Y_FIELDNAME}'.format(**globals()))
        self.assertEqual(r[1], u'3203,SE WOODSTOCK BLVD,-122.629314,45.479425')

    def test_srs(self):
        # This is an example inspired by the hipsters in us-or-portland
        c = {"conform": {"lon": "x", "lat": "y", "srs": "EPSG:2913", "number": "n", "street": "s", "type": "csv"}, 'type': 'test'}
        d = (u'n,s,X,Y'.encode('ascii'),
             u'3203,SE WOODSTOCK BLVD,7655634.924,668868.414'.encode('ascii'))
        r = self._convert(c, d)
        self.assertEqual(r[0], u'n,s,{X_FIELDNAME},{Y_FIELDNAME}'.format(**globals()))
        self.assertEqual(r[1], u'3203,SE WOODSTOCK BLVD,-122.6308422,45.4815544')

    def test_too_many_columns(self):
        "Check that we don't barf on input with too many columns in some rows"
        c = { "conform": { "type": "csv", "lat": "LATITUDE", "lon": "LONGITUDE" }, 'type': 'test' }
        d = (self._ascii_header_in.encode('ascii'),
             self._ascii_row_in.encode('ascii'),
             u'MAPLE ST,123,39.3,-121.2,EXTRY'.encode('ascii'))
        r = self._convert(c, d)
        self.assertEqual(2, len(r))
        self.assertEqual(self._ascii_header_out, r[0])
        self.assertEqual(self._ascii_row_out, r[1])

    def test_esri_csv(self):
        # Test that our ESRI-emitted CSV is converted correctly.
        c = { "type": "ESRI", "conform": { "type": "geojson", "lat": "theseare", "lon": "ignored" } }
        d = (u'STREETNAME,NUMBER,OA:x,OA:y'.encode('ascii'),
             u'MAPLE ST,123,-121.2,39.3'.encode('ascii'))
        r = self._convert(c, d)
        self.assertEqual(self._ascii_header_out, r[0])
        self.assertEqual(self._ascii_row_out, r[1])

    def test_esri_csv_no_lat_lon(self):
        # Test that the ESRI path works even without lat/lon tags. See issue #91
        c = { "type": "ESRI", "conform": { "type": "geojson" } }
        d = (u'STREETNAME,NUMBER,OA:x,OA:y'.encode('ascii'),
             u'MAPLE ST,123,-121.2,39.3'.encode('ascii'))
        r = self._convert(c, d)
        self.assertEqual(self._ascii_header_out, r[0])
        self.assertEqual(self._ascii_row_out, r[1])
