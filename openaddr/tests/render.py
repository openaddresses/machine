from __future__ import division

import os
import unittest
import tempfile
import subprocess

from os.path import join, dirname

from ..render import render

class TestRender (unittest.TestCase):

    def test_render(self):
        sources = join(dirname(__file__), 'sources')
        handle, filename = tempfile.mkstemp(prefix='render-', suffix='.png')
        os.close(handle)
        
        try:
            render(sources, set(), 512, 1, filename)
            info = str(subprocess.check_output(('file', filename)))

            self.assertTrue('PNG image data' in info)
            self.assertTrue('512 x 294' in info)
            self.assertTrue('8-bit/color RGBA' in info)
        finally:
            os.remove(filename)
