# coding=ascii

from __future__ import absolute_import, division, print_function

import os
import copy
import json
import re

import unittest
import tempfile
import shutil

from ..conform import (
    GEOM_FIELDNAME, X_FIELDNAME, Y_FIELDNAME,
    csv_source_to_csv, find_source_path, row_transform_and_convert,
    row_fxn_regexp, row_smash_case, row_round_lat_lon, row_merge,
    row_extract_and_reproject, row_convert_to_out, row_fxn_join, row_fxn_format,
    row_fxn_prefixed_number, row_fxn_postfixed_street,
    row_canonicalize_unit_and_number, conform_smash_case, conform_cli,
    csvopen, csvDictReader, convert_regexp_replace, conform_license,
    conform_attribution, conform_sharealike, normalize_ogr_filename_case,
    OPENADDR_CSV_SCHEMA, is_in, geojson_source_to_csv
    )

class TestConformTransforms (unittest.TestCase):
    "Test low level data transform functions"

    def test_row_smash_case(self):
        r = row_smash_case(None, {"UPPER": "foo", "lower": "bar", "miXeD": "mixed"})
        self.assertEqual({"upper": "foo", "lower": "bar", "mixed": "mixed"}, r)

    def test_conform_smash_case(self):
        d = { "conform": { "street": [ "U", "l", "MiXeD" ], "number": "U", "lat": "Y", "lon": "x",
                           "city": { "function": "join", "fields": ["ThIs","FiELd"], "separator": "-" },
                           "district": { "function": "regexp", "field": "ThaT", "pattern": ""},
                           "postcode": { "function": "join", "fields": ["MiXeD", "UPPER"], "separator": "-" } } }
        r = conform_smash_case(d)
        self.assertEqual({ "conform": { "street": [ "u", "l", "mixed" ], "number": "u", "lat": "y", "lon": "x",
                           "city": {"fields": ["this", "field"], "function": "join", "separator": "-"},
                           "district": { "field": "that", "function": "regexp", "pattern": ""},
                           "postcode": { "function": "join", "fields": ["mixed", "upper"], "separator": "-" } } },
                         r)

    def test_row_convert_to_out(self):
        d = { "conform": { "street": "s", "number": "n" } }
        r = row_convert_to_out(d, {"s": "MAPLE LN", "n": "123", X_FIELDNAME: "-119.2", Y_FIELDNAME: "39.3"})
        self.assertEqual({"LON": "-119.2", "LAT": "39.3", "UNIT": None, "NUMBER": "123", "STREET": "MAPLE LN",
                          "CITY": None, "REGION": None, "DISTRICT": None, "POSTCODE": None, "ID": None}, r)

    def test_row_merge(self):
        d = { "conform": { "street": [ "n", "t" ] } }
        r = row_merge(d, {"n": "MAPLE", "t": "ST", "x": "foo"}, 'street')
        self.assertEqual({"OA:street": "MAPLE ST", "x": "foo", "t": "ST", "n": "MAPLE"}, r)

        d = { "conform": { "city": [ "n", "t" ] } }
        r = row_merge(d, {"n": "Village of", "t": "Stanley", "x": "foo"}, 'city')
        self.assertEqual({"OA:city": "Village of Stanley", "x": "foo", "t": "Stanley", "n": "Village of"}, r)

    def test_row_fxn_join(self):
        "New fxn join"
        c = { "conform": {
            "number": {
                "function": "join",
                "fields": ["a1"]
            },
            "street": {
                "function": "join",
                "fields": ["b1","b2"],
                "separator": "-"
            }
        } }
        d = { "a1": "va1", "b1": "vb1", "b2": "vb2" }
        e = copy.deepcopy(d)
        e.update({ "OA:number": "va1", "OA:street": "vb1-vb2" })
        d = row_fxn_join(c, d, "number")
        d = row_fxn_join(c, d, "street")
        self.assertEqual(e, d)
        d = { "a1": "va1", "b1": "vb1", "b2": None}
        e = copy.deepcopy(d)
        e.update({ "OA:number": "va1", "OA:street": "vb1" })
        d = row_fxn_join(c, d, "number")
        d = row_fxn_join(c, d, "street")
        self.assertEqual(e, d)

    def test_row_fxn_format(self):
        c = { "conform": {
            "number": {
                "function": "format",
                "fields": ["a1", "a2", "a3"],
                "format": "$1-$2-$3"
            },
            "street": {
                "function": "format",
                "fields": ["b1", "b2", "b3"],
                "format": "foo $1$2-$3 bar"
            }
        } }

        d = {"a1": "12", "a2": "34", "a3": "56", "b1": "1", "b2": "B", "b3": "3"}
        e = copy.deepcopy(d)
        d = row_fxn_format(c, d, "number")
        d = row_fxn_format(c, d, "street")
        self.assertEqual(d.get("OA:number", ""), "12-34-56")
        self.assertEqual(d.get("OA:street", ""), "foo 1B-3 bar")

        d = copy.deepcopy(e)
        d["a2"] = None
        d["b3"] = None
        d = row_fxn_format(c, d, "number")
        d = row_fxn_format(c, d, "street")
        self.assertEqual(d.get("OA:number", ""), "12-56")
        self.assertEqual(d.get("OA:street", ""), "foo 1B bar")

    def test_row_fxn_regexp(self):
        "Regex split - replace"
        c = { "conform": {
            "number": {
                "function": "regexp",
                "field": "ADDRESS",
                "pattern": "^([0-9]+)(?:.*)",
                "replace": "$1"
            },
            "street": {
                "function": "regexp",
                "field": "ADDRESS",
                "pattern": "(?:[0-9]+ )(.*)",
                "replace": "$1"
            }
        } }
        d = { "ADDRESS": "123 MAPLE ST" }
        e = copy.deepcopy(d)
        e.update({ "OA:number": "123", "OA:street": "MAPLE ST" })
        
        d = row_fxn_regexp(c, d, "number")
        d = row_fxn_regexp(c, d, "street")
        self.assertEqual(e, d)
        
        "Regex split - no replace - good match"
        c = { "conform": {
            "number": {
                "function": "regexp",
                "field": "ADDRESS",
                "pattern": "^([0-9]+)"
            },
            "street": {
                "function": "regexp",
                "field": "ADDRESS",
                "pattern": "(?:[0-9]+ )(.*)"
            }
        } }
        d = { "ADDRESS": "123 MAPLE ST" }
        e = copy.deepcopy(d)
        e.update({ "OA:number": "123", "OA:street": "MAPLE ST" })
        
        d = row_fxn_regexp(c, d, "number")
        d = row_fxn_regexp(c, d, "street")
        self.assertEqual(e, d)

        "regex split - no replace - bad match"
        c = { "conform": { 
            "number": {
                "function": "regexp",
                "field": "ADDRESS",
                "pattern": "^([0-9]+)"
            },
            "street": {
                "function": "regexp",
                "field": "ADDRESS",
                "pattern": "(fake)"
            }
        } }
        d = { "ADDRESS": "123 MAPLE ST" }
        e = copy.deepcopy(d)
        e.update({ "OA:number": "123", "OA:street": "" })
        
        d = row_fxn_regexp(c, d, "number")
        d = row_fxn_regexp(c, d, "street")
        self.assertEqual(e, d)
        
    def test_transform_and_convert(self):
        d = { "conform": { "street": ["s1", "s2"], "number": "n", "lon": "y", "lat": "x" }, "fingerprint": "0000" }
        r = row_transform_and_convert(d, { "n": "123", "s1": "MAPLE", "s2": "ST", X_FIELDNAME: "-119.2", Y_FIELDNAME: "39.3" })
        self.assertEqual({"STREET": "MAPLE ST", "UNIT": "", "NUMBER": "123", "LON": "-119.2", "LAT": "39.3",
                          "CITY": None, "REGION": None, "DISTRICT": None, "POSTCODE": None, "ID": None,
                          'HASH': 'eee8eb535bb20a03'}, r)

        d = { "conform": { "street": ["s1", "s2"], "number": "n", "lon": "y", "lat": "x" }, "fingerprint": "0000" }
        r = row_transform_and_convert(d, { "n": "123", "s1": "MAPLE", "s2": "ST", X_FIELDNAME: "-119.2", Y_FIELDNAME: "39.3" })
        self.assertEqual({"STREET": "MAPLE ST", "UNIT": "", "NUMBER": "123", "LON": "-119.2", "LAT": "39.3",
                          "CITY": None, "REGION": None, "DISTRICT": None, "POSTCODE": None, "ID": None,
                          'HASH': 'eee8eb535bb20a03'}, r)

        d = { "conform": { "number": {"function": "regexp", "field": "s", "pattern": "^(\\S+)" }, "street": { "function": "regexp", "field": "s", "pattern": "^(?:\\S+ )(.*)" }, "lon": "y", "lat": "x" }, "fingerprint": "0000" }
        r = row_transform_and_convert(d, { "s": "123 MAPLE ST", X_FIELDNAME: "-119.2", Y_FIELDNAME: "39.3" })
        self.assertEqual({"STREET": "MAPLE ST", "UNIT": "", "NUMBER": "123", "LON": "-119.2", "LAT": "39.3",
                          "CITY": None, "REGION": None, "DISTRICT": None, "POSTCODE": None, "ID": None,
                          'HASH': 'eee8eb535bb20a03'}, r)

    def test_row_canonicalize_unit_and_number(self):
        r = row_canonicalize_unit_and_number({}, {"NUMBER": "324 ", "STREET": " OAK DR.", "UNIT": "1"})
        self.assertEqual("324", r["NUMBER"])
        self.assertEqual("OAK DR.", r["STREET"])
        self.assertEqual("1", r["UNIT"])

        # Tests for integer conversion
        for e, a in (("324", " 324.0  "),
                     ("", ""),
                     ("3240", "3240"),
                     ("INVALID", "INVALID"),
                     ("324.5", "324.5")):
            r = row_canonicalize_unit_and_number({}, {"NUMBER": a, "STREET": "", "UNIT": ""})
            self.assertEqual(e, r["NUMBER"])

    def test_row_canonicalize_street_and_no_number(self):
        r = row_canonicalize_unit_and_number({}, {"NUMBER": None, "STREET": " OAK DR.", "UNIT": None})
        self.assertEqual("", r["NUMBER"])
        self.assertEqual("OAK DR.", r["STREET"])
        self.assertEqual("", r["UNIT"])

    def test_row_canonicalize_street_with_no_unit_number(self):
        r = row_canonicalize_unit_and_number({}, {"NUMBER": None, "STREET": " OAK DR.", "UNIT": None})
        self.assertEqual("", r["NUMBER"])
        self.assertEqual("OAK DR.", r["STREET"])
        self.assertEqual("", r["UNIT"])

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

    def test_row_fxn_prefixed_number_and_postfixed_street(self):
        "Regex prefixed_number and postfix_street - both fields present"
        c = { "conform": {
            "number": {
                "function": "prefixed_number",
                "field": "ADDRESS"
            },
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "123 MAPLE ST" }
        e = copy.deepcopy(d)
        e.update({ "OA:number": "123", "OA:street": "MAPLE ST" })

        d = row_fxn_prefixed_number(c, d, "number")
        d = row_fxn_postfixed_street(c, d, "street")
        self.assertEqual(e, d)

        "Regex prefixed_number and postfixed_street - no number"
        c = { "conform": {
            "number": {
                "function": "prefixed_number",
                "field": "ADDRESS"
            },
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "MAPLE ST" }
        e = copy.deepcopy(d)
        e.update({ "OA:number": "", "OA:street": "MAPLE ST" })

        d = row_fxn_prefixed_number(c, d, "number")
        d = row_fxn_postfixed_street(c, d, "street")
        self.assertEqual(e, d)

        "Regex prefixed_number and postfixed_street - empty input"
        c = { "conform": {
            "number": {
                "function": "prefixed_number",
                "field": "ADDRESS"
            },
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "" }
        e = copy.deepcopy(d)
        e.update({ "OA:number": "", "OA:street": "" })

        d = row_fxn_prefixed_number(c, d, "number")
        d = row_fxn_postfixed_street(c, d, "street")
        self.assertEqual(e, d)

        "Regex prefixed_number and postfixed_street - no spaces after number"
        c = { "conform": {
            "number": {
                "function": "prefixed_number",
                "field": "ADDRESS"
            },
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "123MAPLE ST" }
        e = copy.deepcopy(d)
        e.update({ "OA:number": "", "OA:street": "123MAPLE ST" })

        d = row_fxn_prefixed_number(c, d, "number")
        d = row_fxn_postfixed_street(c, d, "street")
        self.assertEqual(e, d)

        "Regex prefixed_number and postfixed_street - excess whitespace"
        c = { "conform": {
            "number": {
                "function": "prefixed_number",
                "field": "ADDRESS"
            },
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": " \t 123 \t MAPLE ST" }
        e = copy.deepcopy(d)
        e.update({ "OA:number": "123", "OA:street": "MAPLE ST" })

        d = row_fxn_prefixed_number(c, d, "number")
        d = row_fxn_postfixed_street(c, d, "street")
        self.assertEqual(e, d)

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
            self.assertEqual(OPENADDR_CSV_SCHEMA, reader.fieldnames)

            rows = list(reader)

            self.assertAlmostEqual(float(rows[0]['LAT']), 37.802612637607439)
            self.assertAlmostEqual(float(rows[0]['LON']), -122.259249687194824)

            self.assertEqual(6, len(rows))
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

    def test_lake_man_gdb(self):
        rc, dest_path = self._run_conform_on_source('lake-man-gdb', 'gdb')
        self.assertEqual(0, rc)

        with csvopen(dest_path) as fp:
            reader = csvDictReader(fp)
            self.assertEqual(OPENADDR_CSV_SCHEMA, reader.fieldnames)

            rows = list(reader)

            self.assertAlmostEqual(float(rows[0]['LAT']), 37.802612637607439)
            self.assertAlmostEqual(float(rows[0]['LON']), -122.259249687194824)

            self.assertEqual(6, len(rows))
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
        rc, dest_path = self._run_conform_on_source('lake-man-split', 'shp')
        self.assertEqual(0, rc)
        
        with csvopen(dest_path) as fp:
            rows = list(csvDictReader(fp))
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
        rc, dest_path = self._run_conform_on_source('lake-man-merge-postcode', 'shp')
        self.assertEqual(0, rc)
        
        with csvopen(dest_path) as fp:
            rows = list(csvDictReader(fp))
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
        rc, dest_path = self._run_conform_on_source('lake-man-merge-postcode2', 'shp')
        self.assertEqual(0, rc)
        
        with csvopen(dest_path) as fp:
            rows = list(csvDictReader(fp))
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

    def test_lake_man_shp_utf8(self):
        rc, dest_path = self._run_conform_on_source('lake-man-utf8', 'shp')
        self.assertEqual(0, rc)
        with csvopen(dest_path, encoding='utf-8') as fp:
            rows = list(csvDictReader(fp, encoding='utf-8'))
            self.assertEqual(rows[0]['STREET'], u'PZ ESPA\u00d1A')

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
            self.assertEqual(rows[0]['STREET'], 'Spectrum Pointe Dr #320')
            self.assertEqual(rows[1]['NUMBER'], '')
            self.assertEqual(rows[1]['STREET'], '')
            self.assertEqual(rows[2]['NUMBER'], '300')
            self.assertEqual(rows[2]['STREET'], 'E Chapman Ave')
            self.assertEqual(rows[3]['NUMBER'], '1')
            self.assertEqual(rows[3]['STREET'], 'Spectrum Pointe Dr #320')
            self.assertEqual(rows[4]['NUMBER'], '1')
            self.assertEqual(rows[4]['STREET'], 'Spectrum Pointe Dr #320')
            self.assertEqual(rows[5]['NUMBER'], '1')
            self.assertEqual(rows[5]['STREET'], 'Spectrum Pointe Dr #320')

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
            self.assertEqual(rows[0]['STREET'], u'PZ ESPA\u00d1A')

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
            self.assertEqual(rows[0]['STREET'], 'FRUITED PLAINS LN')


class TestConformMisc(unittest.TestCase):

    def setUp(self):
        self.testdir = tempfile.mkdtemp(prefix='openaddr-TestConformMisc-')

    def tearDown(self):
        shutil.rmtree(self.testdir)

    def test_convert_regexp_replace(self):
        '''
        '''
        crr = convert_regexp_replace
        
        self.assertEqual(crr('$1'), r'\1')
        self.assertEqual(crr('$9'), r'\9')
        self.assertEqual(crr('$b'), '$b')
        self.assertEqual(crr('$1yo$1'), r'\1yo\1')
        self.assertEqual(crr('$9yo$9'), r'\9yo\9')
        self.assertEqual(crr('$byo$b'), '$byo$b')
        self.assertEqual(crr('$1 yo $1'), r'\1 yo \1')
        self.assertEqual(crr('$9 yo $9'), r'\9 yo \9')
        self.assertEqual(crr('$b yo $b'), '$b yo $b')

        self.assertEqual(crr('$11'), r'\11')
        self.assertEqual(crr('$99'), r'\99')
        self.assertEqual(crr('$bb'), '$bb')
        self.assertEqual(crr('$11yo$11'), r'\11yo\11')
        self.assertEqual(crr('$99yo$99'), r'\99yo\99')
        self.assertEqual(crr('$bbyo$bb'), '$bbyo$bb')
        self.assertEqual(crr('$11 yo $11'), r'\11 yo \11')
        self.assertEqual(crr('$99 yo $99'), r'\99 yo \99')
        self.assertEqual(crr('$bb yo $bb'), '$bb yo $bb')

        self.assertEqual(crr('${1}1'), r'\g<1>1')
        self.assertEqual(crr('${9}9'), r'\g<9>9')
        self.assertEqual(crr('${9}b'), r'\g<9>b')
        self.assertEqual(crr('${b}b'), '${b}b')
        self.assertEqual(crr('${1}1yo${1}1'), r'\g<1>1yo\g<1>1')
        self.assertEqual(crr('${9}9yo${9}9'), r'\g<9>9yo\g<9>9')
        self.assertEqual(crr('${9}byo${9}b'), r'\g<9>byo\g<9>b')
        self.assertEqual(crr('${b}byo${b}b'), '${b}byo${b}b')
        self.assertEqual(crr('${1}1 yo ${1}1'), r'\g<1>1 yo \g<1>1')
        self.assertEqual(crr('${9}9 yo ${9}9'), r'\g<9>9 yo \g<9>9')
        self.assertEqual(crr('${9}b yo ${9}b'), r'\g<9>b yo \g<9>b')
        self.assertEqual(crr('${b}b yo ${b}b'), '${b}b yo ${b}b')

        self.assertEqual(crr('${11}1'), r'\g<11>1')
        self.assertEqual(crr('${99}9'), r'\g<99>9')
        self.assertEqual(crr('${99}b'), r'\g<99>b')
        self.assertEqual(crr('${bb}b'), '${bb}b')
        self.assertEqual(crr('${11}1yo${11}1'), r'\g<11>1yo\g<11>1')
        self.assertEqual(crr('${99}9yo${99}9'), r'\g<99>9yo\g<99>9')
        self.assertEqual(crr('${99}byo${99}b'), r'\g<99>byo\g<99>b')
        self.assertEqual(crr('${bb}byo${bb}b'), '${bb}byo${bb}b')
        self.assertEqual(crr('${11}1yo${11}1'), r'\g<11>1yo\g<11>1')
        self.assertEqual(crr('${99}9 yo ${99}9'), r'\g<99>9 yo \g<99>9')
        self.assertEqual(crr('${99}b yo ${99}b'), r'\g<99>b yo \g<99>b')
        self.assertEqual(crr('${bb}b yo ${bb}b'), '${bb}b yo ${bb}b')
        
        self.assertEqual(re.sub(r'hello (world)', crr('goodbye $1'), 'hello world'), 'goodbye world')
        self.assertEqual(re.sub(r'(hello) (world)', crr('goodbye $2'), 'hello world'), 'goodbye world')
        self.assertEqual(re.sub(r'he(ll)o', crr('he$1$1o'), 'hello'), 'hellllo')

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

    def test_find_gdb_source_path(self):
        shp_conform = {"conform": { "type": "gdb" } }
        self.assertEqual("foo.gdb", find_source_path(shp_conform, ["foo.gdb"]))
        self.assertEqual("FOO.GDB", find_source_path(shp_conform, ["FOO.GDB"]))
        self.assertEqual("xyzzy/FOO.GDB", find_source_path(shp_conform, ["xyzzy/FOO.GDB"]))
        self.assertEqual("foo.gdb", find_source_path(shp_conform, ["foo.gdb", "foo.prj", "foo.shx"]))
        self.assertEqual(None, find_source_path(shp_conform, ["nope.txt"]))
        self.assertEqual(None, find_source_path(shp_conform, ["foo.gdb", "bar.gdb"]))

        shp_file_conform = {"conform": { "type": "gdb", "file": "foo.gdb" } }
        self.assertEqual("foo.gdb", find_source_path(shp_file_conform, ["foo.gdb"]))
        self.assertEqual("foo.gdb", find_source_path(shp_file_conform, ["foo.gdb", "bar.gdb"]))
        self.assertEqual("xyzzy/foo.gdb", find_source_path(shp_file_conform, ["xyzzy/foo.gdb", "xyzzy/bar.gdb"]))

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

    def test_normalize_ogr_filename_case1(self):
        filename = os.path.join(self.testdir, 'file.geojson')
        with open(filename, 'w') as file:
            file.write('yo')
        
        self.assertEqual(normalize_ogr_filename_case(filename), filename)
        self.assertTrue(os.path.exists(normalize_ogr_filename_case(filename)))

    def test_normalize_ogr_filename_case2(self):
        filename = os.path.join(self.testdir, 'file.GeoJSON')
        with open(filename, 'w') as file:
            file.write('yo')
        
        self.assertNotEqual(normalize_ogr_filename_case(filename), filename)
        self.assertTrue(os.path.exists(normalize_ogr_filename_case(filename)))

    def test_normalize_ogr_filename_case3(self):
        filename = os.path.join(self.testdir, 'file.shp')
        with open(filename, 'w') as file:
            file.write('yo')
        
        for otherbase in ('file.shx', 'file.dbf', 'file.prj'):
            othername = os.path.join(self.testdir, otherbase)
            with open(othername, 'w') as other:
                other.write('yo')
        
        self.assertEqual(normalize_ogr_filename_case(filename), filename)
        self.assertTrue(os.path.exists(normalize_ogr_filename_case(filename)))
        self.assertTrue(os.path.exists(os.path.join(self.testdir, 'file.shx')))
        self.assertTrue(os.path.exists(os.path.join(self.testdir, 'file.dbf')))
        self.assertTrue(os.path.exists(os.path.join(self.testdir, 'file.prj')))

    def test_normalize_ogr_filename_case4(self):
        filename = os.path.join(self.testdir, 'file.Shp')
        with open(filename, 'w') as file:
            file.write('yo')
        
        for otherbase in ('file.Shx', 'file.Dbf', 'file.Prj'):
            othername = os.path.join(self.testdir, otherbase)
            with open(othername, 'w') as other:
                other.write('yo')
        
        self.assertNotEqual(normalize_ogr_filename_case(filename), filename)
        self.assertTrue(os.path.exists(normalize_ogr_filename_case(filename)))
        self.assertTrue(os.path.exists(os.path.join(self.testdir, 'file.shx')))
        self.assertTrue(os.path.exists(os.path.join(self.testdir, 'file.dbf')))
        self.assertTrue(os.path.exists(os.path.join(self.testdir, 'file.prj')))

    def test_normalize_ogr_filename_case5(self):
        filename = os.path.join(self.testdir, 'file.SHP')
        with open(filename, 'w') as file:
            file.write('yo')
        
        for otherbase in ('file.SHX', 'file.DBF', 'file.PRJ'):
            othername = os.path.join(self.testdir, otherbase)
            with open(othername, 'w') as other:
                other.write('yo')
        
        self.assertNotEqual(normalize_ogr_filename_case(filename), filename)
        self.assertTrue(os.path.exists(normalize_ogr_filename_case(filename)))
        self.assertTrue(os.path.exists(os.path.join(self.testdir, 'file.shx')))
        self.assertTrue(os.path.exists(os.path.join(self.testdir, 'file.dbf')))
        self.assertTrue(os.path.exists(os.path.join(self.testdir, 'file.prj')))
    
    def test_is_not_in(self):
        self.assertFalse(is_in('foo', []), 'Should not match an empty list')
        self.assertFalse(is_in('foo', ['bar']), 'Should not match')
        self.assertTrue(is_in('foo', ['foo']), 'Should be a simple match')
        self.assertTrue(is_in('Foo', ['foo']), 'Should be a case-insensitive match')

        self.assertFalse(is_in('foo/bar', ['bar']), 'Should not match in a directory')
        self.assertTrue(is_in('foo/bar', ['foo']), 'Should match a directory name')
        self.assertTrue(is_in('Foo/bar', ['foo']), 'Should match a directory case-insensitively')

        self.assertFalse(is_in('foo/bar/baz', ['baz']), 'Should not match in a nested directory')
        self.assertTrue(is_in('foo/bar', ['foo/bar']), 'Should match a directory path')
        self.assertTrue(is_in('foo/bar/baz', ['foo/bar']), 'Should match a directory path')
        self.assertTrue(is_in('foo/bar/baz', ['foo']), 'Should match a directory path')
        self.assertTrue(is_in('Foo/bar/baz', ['foo']), 'Should match a directory path case-insensitively')
        self.assertTrue(is_in('foo/Bar', ['foo/bar']), 'Should match a directory path case-insensitively')
        self.assertTrue(is_in('foo/Bar/baz', ['foo/bar']), 'Should match a directory path case-insensitively')
    
    def test_geojson_source_to_csv(self):
        '''
        '''
        geojson_path = os.path.join(os.path.dirname(__file__), 'data/us-pa-bucks.geojson')
        csv_path = os.path.join(self.testdir, 'us-tx-waco.csv')
        geojson_source_to_csv(geojson_path, csv_path)
        
        with csvopen(csv_path, encoding='utf8') as file:
            row = next(csvDictReader(file, encoding='utf8'))
            self.assertAlmostEqual(float(row[X_FIELDNAME]), -74.98335721879076)
            self.assertAlmostEqual(float(row[Y_FIELDNAME]), 40.054962450263616)
            self.assertEqual(row['PARCEL_NUM'], '02-022-003')

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

class TestConformLicense (unittest.TestCase):

    def test_license_string(self):
        ''' Test that simple license strings are converted correctly.
        '''
        self.assertIsNone(conform_license(None))
        self.assertEqual(conform_license('CC-BY-SA'), 'CC-BY-SA')
        self.assertEqual(conform_license('http://example.com'), 'http://example.com')
        self.assertEqual(conform_license(u'\xa7 unicode \xa7'), u'\xa7 unicode \xa7')

    def test_license_dictionary(self):
        ''' Test that simple license strings are converted correctly.
        '''
        self.assertIsNone(conform_license({}))
        self.assertEqual(conform_license({'text': 'CC-BY-SA'}), 'CC-BY-SA')
        self.assertEqual(conform_license({'url': 'http://example.com'}), 'http://example.com')
        self.assertEqual(conform_license({'text': u'\xa7 unicode \xa7'}), u'\xa7 unicode \xa7')
        
        license = {'text': 'CC-BY-SA', 'url': 'http://example.com'}
        self.assertIn(license['text'], conform_license(license))
        self.assertIn(license['url'], conform_license(license))
    
    def test_attribution(self):
        ''' Test combinations of attribution data.
        '''
        attr_flag1, attr_name1 = conform_attribution(None, None)
        self.assertIs(attr_flag1, False)
        self.assertIsNone(attr_name1)

        attr_flag2, attr_name2 = conform_attribution({}, None)
        self.assertIs(attr_flag2, False)
        self.assertIsNone(attr_name2)

        attr_flag3, attr_name3 = conform_attribution(None, '')
        self.assertIs(attr_flag3, False)
        self.assertIsNone(attr_name3)

        attr_flag4, attr_name4 = conform_attribution({}, '')
        self.assertIs(attr_flag4, False)
        self.assertIsNone(attr_name4)

        attr_flag5, attr_name5 = conform_attribution(None, u'Joe Bl\xf6')
        self.assertIs(attr_flag5, True)
        self.assertEqual(attr_name5, u'Joe Bl\xf6')

        attr_flag6, attr_name6 = conform_attribution({}, u'Joe Bl\xf6')
        self.assertIs(attr_flag6, True)
        self.assertEqual(attr_name6, u'Joe Bl\xf6')

        attr_flag7, attr_name7 = conform_attribution({'attribution': False}, u'Joe Bl\xf6')
        self.assertIs(attr_flag7, False)
        self.assertEqual(attr_name7, None)

        attr_flag8, attr_name8 = conform_attribution({'attribution': True}, u'Joe Bl\xf6')
        self.assertIs(attr_flag8, True)
        self.assertEqual(attr_name8, u'Joe Bl\xf6')

        attr_flag9, attr_name9 = conform_attribution({'attribution': None}, u'Joe Bl\xf6')
        self.assertIs(attr_flag9, True)
        self.assertEqual(attr_name9, u'Joe Bl\xf6')

        attr_flag10, attr_name10 = conform_attribution({'attribution': False, 'attribution name': u'Joe Bl\xf6'}, None)
        self.assertIs(attr_flag10, False)
        self.assertEqual(attr_name10, None)

        attr_flag11, attr_name11 = conform_attribution({'attribution': True, 'attribution name': u'Joe Bl\xf6'}, None)
        self.assertIs(attr_flag11, True)
        self.assertEqual(attr_name11, u'Joe Bl\xf6')

        attr_flag12, attr_name12 = conform_attribution({'attribution': None, 'attribution name': u'Joe Bl\xf6'}, None)
        self.assertIs(attr_flag12, True)
        self.assertEqual(attr_name12, u'Joe Bl\xf6')

        attr_flag13, attr_name13 = conform_attribution({'attribution': None, 'attribution name': u'Joe Bl\xf6'}, 'Jon Snow')
        self.assertIs(attr_flag13, True)
        self.assertEqual(attr_name13, u'Joe Bl\xf6')

        attr_flag14, attr_name14 = conform_attribution({'attribution': None, 'attribution name': False}, None)
        self.assertIs(attr_flag14, True)
        self.assertEqual(attr_name14, 'False')
    
    def test_sharealike(self):
        ''' Test combinations of share=alike data.
        '''
        for undict in (None, False, True, 'this', 'that'):
            self.assertIs(conform_sharealike(undict), None, '{} should be None'.format(undict))
        
        for value1 in (False, 'No', 'no', 'false', 'False', 'n', 'f', None, ''):
            dict1 = {'share-alike': value1}
            self.assertIs(conform_sharealike(dict1), False, 'sa:{} should be False'.format(repr(value1)))

        for value2 in (True, 'Yes', 'yes', 'true', 'True', 'y', 't'):
            dict2 = {'share-alike': value2}
            self.assertIs(conform_sharealike(dict2), True, 'sa:{} should be True'.format(repr(value2)))
