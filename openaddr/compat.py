import sys

PY2 = (sys.version_info[0] == 2)

if PY2:
    import unicodecsv as csv
    csv.field_size_limit(sys.maxsize)
    
    from future import standard_library
    standard_library.install_aliases()

else:
    import csv
    standard_library = None

try:
    import cairo
except ImportError:
    # http://stackoverflow.com/questions/11491268/install-pycairo-in-virtualenv
    import cairocffi as cairo
