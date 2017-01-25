import sys
import gzip
import io

PY2 = (sys.version_info[0] == 2)

if PY2:
    from future import standard_library
    standard_library.install_aliases()

else:
    import csv, subprocess
    standard_library = None

try:
    import cairo
except ImportError:
    # http://stackoverflow.com/questions/11491268/install-pycairo-in-virtualenv
    import cairocffi as cairo
