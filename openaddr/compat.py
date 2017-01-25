import sys
import gzip
import io

PY2 = (sys.version_info[0] == 2)

if PY2:
    from pipes import quote
    import uritemplate
    
    def gzopen(filename, mode='r', encoding=None):
        ''' Discard encoding
        '''
        return gzip.open(filename, mode=mode)
    
    def expand_uri(template, args):
        '''
        '''
        new_args = {k: v for (k, v) in args.items() if not hasattr(v, 'encode')}
        new_args.update({k: v.encode('utf8') for (k, v) in args.items() if hasattr(v, 'encode')})
        
        return uritemplate.expand(template, new_args)
    
    from future import standard_library
    standard_library.install_aliases()

else:
    from shlex import quote
    import csv, subprocess
    from uritemplate import expand as expand_uri
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
