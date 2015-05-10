import unittest

from ..expand import expand_street_name

class TestExpand(unittest.TestCase):
    def test_expand_street_name(self):
        for e, a in (
            (None, None),
            ("", ""),
            ("Oak Drive", "OAK DR"),
            ("Oak Drive", "  OAK DR "),
            ("Oak Drive", "OAK DR."),
            ("Mornington Crescent", "MORNINGTON CR"),
        ):
            self.assertEqual(e, expand_street_name(a))

    def test_expand_street_name_st(self):
        for e, a in (
            ("Maple Street", "MAPLE ST"),
            ("Saint Isidore Drive", "ST ISIDORE DR"),
            ("Saint Sebastian Street", "ST. Sebastian ST"),
            ("Mornington Crescent", "MORNINGTON CR"),
        ):
            self.assertEqual(e, expand_street_name(a))

    def test_expand_case_exceptions(self):
        for e, a in (
            ("3rd Street", "3RD ST"),
        ):
            self.assertEqual(e, expand_street_name(a))
