import sys

PY2 = (sys.version_info[0] == 2)

if PY2:
    import unicodecsv as csv
    csv.field_size_limit(sys.maxsize)
    
    from thread import get_ident as thread_ident
    
    from future import standard_library
    standard_library.install_aliases()

else:
    import csv
    from threading import get_ident as thread_ident
    
    standard_library = None
