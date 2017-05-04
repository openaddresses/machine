# coding=utf8
from __future__ import division

import os
import unittest
import tempfile
import subprocess

from os.path import join, dirname

from .. import render
from mock import patch, Mock

from httmock import HTTMock, response

class TestRender (unittest.TestCase):

    def test_render_png(self):
        sources = join(dirname(__file__), 'sources')
        handle, filename = tempfile.mkstemp(prefix='render-', suffix='.png')
        os.close(handle)
        
        try:
            render.render_png(sources, set(), 512, 1, filename, render.WORLD)
            info = str(subprocess.check_output(('file', filename)))

            self.assertIn('PNG image data', info)
            self.assertIn('512 x 294', info)
            self.assertIn('8-bit/color RGBA', info)
        finally:
            os.remove(filename)
    
    def test_render_geojson(self):
        sources = join(dirname(__file__), 'sources')
        handle, filename = tempfile.mkstemp(prefix='render-', suffix='.geojson')
        os.close(handle)
        
        try:
            render.render_geojson(sources, dict(), filename, render.WORLD)
            with open(filename) as file:
                content = file.read()

            self.assertIn('"type": "FeatureCollection"', content)
            self.assertIn('"status": "bad"', content)
            self.assertNotIn('"status": "good"', content, "They're all bad")
        finally:
            os.remove(filename)
    
    def test_load_live_state(self):
        def response_state_txt(url, request):
            if (url.hostname, url.path) == ('results.openaddresses.io', '/state.txt'):
                data = u'''source	cache	sample	geometry type	address count	version	fingerprint	cache time	processed	process time	process hash	output	attribution required	attribution name	share-alike	code version\nar/ba/buenos_aires.json	http://data.openaddresses.io/runs/133863/cache.csv	http://data.openaddresses.io/runs/133863/sample.json		555755		e76afd805a12d38e761bdba62f3ed9cd	0:00:53.490744	http://data.openaddresses.io/runs/133863/ar/ba/buenos_aires.zip	0:03:09.694817	f23a64c8150b6444a7996900d1d8321e	http://data.openaddresses.io/runs/133863/output.txt	true	SOURCE: Government of the Autonomous City of Buenos Aires, downloaded December 2014.		3.14.0\nat/31254.json	http://data.openaddresses.io/runs/134569/cache.zip	http://data.openaddresses.io/runs/134569/sample.json		218885		f06a9cd17511a88bfdd208b79137fa03	0:00:01.749031	http://data.openaddresses.io/runs/134569/at/31254.zip	0:01:22.929958	d6882314c01ad17f84930dec486f040d	http://data.openaddresses.io/runs/134569/output.txt	true	© Austrian address register, date data from 15.07.2015		3.13.1\nat/31255.json	http://data.openaddresses.io/runs/134015/cache.zip	http://data.openaddresses.io/runs/134015/sample.json		910854		f06a9cd17511a88bfdd208b79137fa03	0:00:01.712412	http://data.openaddresses.io/runs/134015/at/31255.zip	0:05:44.075285	e0f0cdcbfbbcfdbadb21d951c29b6ba5	http://data.openaddresses.io/runs/134015/output.txt	true	© Austrian address register, date data from 15.07.2015		3.14.0\n'''
                return response(200, data.encode('utf8'))

            raise Exception(url)
    
        with HTTMock(response_state_txt):
            state = render.load_live_state()
        
        self.assertEqual(len(state), 3)
        self.assertIn('ar/ba/buenos_aires.json', state)
        self.assertIn('at/31255.json', state)
        self.assertIn('at/31254.json', state)
        self.assertEqual(state['ar/ba/buenos_aires.json'].state.address_count, '555755')
        self.assertEqual(state['at/31255.json'].state.address_count, '910854')
        self.assertEqual(state['at/31254.json'].state.address_count, '218885')
