import unittest
from os import remove, environ
from tempfile import mkdtemp
from shutil import rmtree

from ..ci import dashboard_stats, recreate_db, objects
from . import FakeS3

import psycopg2

DATABASE_URL = environ.get('DATABASE_URL', 'postgres:///hooked_on_sources')

class TestDashboardStats (unittest.TestCase):
    
    def setUp(self):
        '''
        '''
        recreate_db.recreate(DATABASE_URL)
        self.database_url = DATABASE_URL
        self.s3 = FakeS3()
    
    def tearDown(self):
        '''
        '''
        remove(self.s3._fake_keys)
    
    def test_make_stats(self):
        with psycopg2.connect(self.database_url) as conn:
            with conn.cursor() as db:
                set = objects.add_set(db, 'openaddresses', 'openaddresses')

                run1_id = objects.add_run(db)
                state1 = objects.RunState({'address count': 1, 'process time': '0:00:01', 'cache time': '0:00:02'})
                objects.set_run(db, run1_id, 'sources/us/ca/alameda.json', 'abc',
                                b'', state1, True, None, None, 'def', True, set.id)
                
                run2_id = objects.add_run(db)
                state2 = objects.RunState({'address count': 2, 'process time': '0:00:04', 'cache time': '0:00:08'})
                objects.set_run(db, run2_id, 'sources/us/ca/alameda.json', 'ghi',
                                b'', state2, True, None, None, 'jkl', True, set.id)
                
                run3_id = objects.add_run(db)
                state3 = objects.RunState({'address count': 4, 'process time': '0:00:16', 'cache time': '0:00:32'})
                objects.set_run(db, run3_id, 'sources/us/ca/alameda.json', 'mno',
                                b'', state3, True, None, None, 'pqr', True, set.id)
                
                objects.complete_set(db, set.id, 'xyz')

                stats = dashboard_stats.make_stats(db)
        
        self.assertEqual(stats['last_process_times'], [1.0, 4.0, 16.0])
        self.assertEqual(stats['last_cache_times'], [2.0, 8.0, 32.0])
        self.assertEqual(stats['last_address_counts'], [1, 2, 4])
    
    def test_upload_stats(self):
        url = dashboard_stats.upload_stats(self.s3, {'hello': 'world'})
        self.assertEqual(url, 'https://s3.amazonaws.com/fake-bucket/machine-stats.json')
        self.assertEqual(self.s3._read_fake_key('machine-stats.json'), '{"hello": "world"}')
