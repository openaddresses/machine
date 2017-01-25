import sys
import gzip
import io

PY2 = (sys.version_info[0] == 2)

if PY2:
    pass

else:
    import csv, subprocess

try:
    import cairo
except ImportError:
    # http://stackoverflow.com/questions/11491268/install-pycairo-in-virtualenv
    import cairocffi as cairo
