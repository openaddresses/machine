# coding=utf8
# Test suite. This code could be in a separate file

from shutil import rmtree
from os.path import dirname, join
from datetime import datetime
from shlex import quote

import unittest, tempfile, json, io
from mimetypes import guess_type
from urllib.parse import urlparse, parse_qs
from httmock import HTTMock, response
from mock import Mock, patch

from .. import util, ci, LocalProcessedResult, __version__

class TestUtilities (unittest.TestCase):

    def test_db_kwargs(self):
        '''
        '''
        dsn1 = 'postgres://who@where.kitchen/what'
        kwargs1 = util.prepare_db_kwargs(dsn1)
        self.assertEqual(kwargs1['user'], 'who')
        self.assertIsNone(kwargs1['password'])
        self.assertEqual(kwargs1['host'], 'where.kitchen')
        self.assertIsNone(kwargs1['port'])
        self.assertEqual(kwargs1['database'], 'what')
        self.assertNotIn('sslmode', kwargs1)

        dsn2 = 'postgres://who:open-sesame@where.kitchen:5432/what?sslmode=require'
        kwargs2 = util.prepare_db_kwargs(dsn2)
        self.assertEqual(kwargs2['user'], 'who')
        self.assertEqual(kwargs2['password'], 'open-sesame')
        self.assertEqual(kwargs2['host'], 'where.kitchen')
        self.assertEqual(kwargs2['port'], 5432)
        self.assertEqual(kwargs2['database'], 'what')
        self.assertEqual(kwargs2['sslmode'], 'require')

    def test_autoscale(self):
        '''
        '''
        autoscale, cloudwatch, as_group = Mock(), Mock(), Mock()
        group_name = 'CI Workers {0}.x'.format(*__version__.split('.'))

        cloudwatch.get_metric_statistics.return_value = [{}]
        autoscale.get_all_groups.return_value = [as_group]

        as_group.desired_capacity = 2
        util.set_autoscale_capacity(autoscale, cloudwatch, 'ns', 1)

        # The right group name was used.
        autoscale.get_all_groups.assert_called_once_with([group_name])

        # Conditions haven't yet required a capacity increase.
        as_group.set_capacity.assert_not_called()

        as_group.desired_capacity = 1
        util.set_autoscale_capacity(autoscale, cloudwatch, 'ns', 1)

        as_group.desired_capacity = 0
        cloudwatch.get_metric_statistics.return_value = [{'Maximum': 0}]
        util.set_autoscale_capacity(autoscale, cloudwatch, 'ns', 1)

        cloudwatch.get_metric_statistics.return_value = [{'Maximum': 1}]
        util.set_autoscale_capacity(autoscale, cloudwatch, 'ns', 1)

        # Capacity had to be increased to 1.
        as_group.set_capacity.assert_called_once_with(1)

        as_group.desired_capacity = 1
        util.set_autoscale_capacity(autoscale, cloudwatch, 'ns', 2)

        # Capacity had to be increased to 2.
        as_group.set_capacity.assert_called_with(2)

        # The right namespace was used.
        for mock_call in cloudwatch.mock_calls:
            self.assertEqual(mock_call[1][:1], (10800, ))
            self.assertEqual(mock_call[1][-3:], ('tasks queue', 'ns', 'Maximum'))

    def test_summarize_result_licenses(self):
        '''
        '''
        s1 = {'license': 'ODbL', 'attribution name': 'ABC Co.'}
        s2 = {'website': 'http://example.com', 'attribution flag': 'false'}
        s3 = {'attribution flag': 'true', 'attribution name': ''}
        r1 = LocalProcessedResult('abc', 'abc.zip', ci.objects.RunState(s1), None)
        r2 = LocalProcessedResult('def', 'def.zip', ci.objects.RunState(s2), None)
        r3 = LocalProcessedResult('ghi', 'ghi.zip', ci.objects.RunState(s3), None)

        content = util.summarize_result_licenses((r1, r2, r3))

        self.assertIn('abc\nWebsite: Unknown\nLicense: ODbL\nRequired attribution: ABC Co.\n', content)
        self.assertIn('def\nWebsite: http://example.com\nLicense: Unknown\nRequired attribution: No\n', content)
        self.assertIn('ghi\nWebsite: Unknown\nLicense: Unknown\nRequired attribution: Yes\n', content)

    def test_request_ftp_file(self):
        '''
        '''
        data_sources = [
            # Two working cases based on real data
            (join(dirname(__file__), 'data', 'us-or-portland.zip'), 'ftp://ftp02.portlandoregon.gov/CivicApps/address.zip'),
            (join(dirname(__file__), 'data', 'us-ut-excerpt.zip'), 'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/LOCATION/UnpackagedData/AddressPoints/_Statewide/AddressPoints_shp.zip'),

            # Some additional special cases
            (None, 'ftp://ftp02.portlandoregon.gov/CivicApps/address-fake.zip'),
            (None, 'ftp://username:password@ftp02.portlandoregon.gov/CivicApps/address-fake.zip'),
            ]

        for (zip_path, ftp_url) in data_sources:
            parsed = urlparse(ftp_url)

            with patch('ftplib.FTP') as FTP:
                if zip_path is None:
                    zip_bytes = None
                else:
                    with open(zip_path, 'rb') as zip_file:
                        zip_bytes = zip_file.read()

                cb_file = io.BytesIO()
                FTP.return_value.retrbinary.side_effect = lambda cmd, cb: cb_file.write(zip_bytes)

                with patch('openaddr.util.build_request_ftp_file_callback') as build_request_ftp_file_callback:
                    build_request_ftp_file_callback.return_value = cb_file, None
                    resp = util.request_ftp_file(ftp_url)

                FTP.assert_called_once_with(parsed.hostname)
                FTP.return_value.login.assert_called_once_with(parsed.username, parsed.password)
                FTP.return_value.retrbinary.assert_called_once_with('RETR {}'.format(parsed.path), None)

                if zip_bytes is None:
                    self.assertEqual(resp.status_code, 400, 'Nothing to return means failure')
                else:
                    self.assertEqual(resp.status_code, 200)
                    self.assertEqual(resp.content, zip_bytes, 'Expected number of bytes')

    def test_s3_key_url(self):
        '''
        '''
        key1 = Mock()
        key1.name, key1.bucket.name = 'key1', 'bucket1'
        self.assertEqual(util.s3_key_url(key1), 'https://s3.amazonaws.com/bucket1/key1')

        key2 = Mock()
        key2.name, key2.bucket.name = '/key2', 'bucket2'
        self.assertEqual(util.s3_key_url(key2), 'https://s3.amazonaws.com/bucket2/key2')

        key3 = Mock()
        key3.name, key3.bucket.name = 'key/3', 'bucket3'
        self.assertEqual(util.s3_key_url(key3), 'https://s3.amazonaws.com/bucket3/key/3')

        key4 = Mock()
        key4.name, key4.bucket.name = '/key/4', 'bucket4'
        self.assertEqual(util.s3_key_url(key4), 'https://s3.amazonaws.com/bucket4/key/4')

        key5 = Mock()
        key5.name, key5.bucket.name = u'kéy5', 'bucket5'
        self.assertEqual(util.s3_key_url(key5), u'https://s3.amazonaws.com/bucket5/kéy5')

        key6 = Mock()
        key6.name, key6.bucket.name = u'/kéy6', 'bucket6'
        self.assertEqual(util.s3_key_url(key6), u'https://s3.amazonaws.com/bucket6/kéy6')

    def test_log_current_usage(self):
        '''
        '''
        with patch('openaddr.util.get_pidlist') as get_pidlist, \
             patch('openaddr.util.get_cpu_times') as get_cpu_times, \
             patch('openaddr.util.get_diskio_bytes') as get_diskio_bytes, \
             patch('openaddr.util.get_network_bytes') as get_network_bytes, \
             patch('openaddr.util.get_memory_usage') as get_memory_usage:
            get_cpu_times.return_value = 1, 2, 3
            get_diskio_bytes.return_value = 4, 5
            get_network_bytes.return_value = 6, 7
            get_memory_usage.return_value = 8

            previous = util.log_current_usage(0, 0, 0, 0, 0, 0, 0, 0, 0)

        get_cpu_times.assert_called_once_with(get_pidlist.return_value)
        get_diskio_bytes.assert_called_once_with(get_pidlist.return_value)
        get_network_bytes.assert_called_once_with()
        get_memory_usage.assert_called_once_with(get_pidlist.return_value)

        self.assertEqual(previous[:7], (2, 3, 1, 4, 5, 6, 7))
