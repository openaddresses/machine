import sys
import gzip
import io

PY2 = (sys.version_info[0] == 2)

if PY2:
    from pipes import quote
    
    def gzopen(filename, mode='r', encoding=None):
        ''' Discard encoding
        '''
        return gzip.open(filename, mode=mode)
    
    from future import standard_library
    standard_library.install_aliases()

else:
    from shlex import quote
    import csv, subprocess
    standard_library = None
    
    def gzopen(filename, mode='r', encoding=None):
        ''' Pass encoding to gzip.open
        '''
        return gzip.open(filename, mode=mode, encoding=encoding)

try:
    import cairo
except ImportError:
    # http://stackoverflow.com/questions/11491268/install-pycairo-in-virtualenv
    import cairocffi as cairo
