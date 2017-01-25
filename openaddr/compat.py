import sys
import gzip
import io

PY2 = (sys.version_info[0] == 2)

if PY2:
    pass

else:
    import csv, subprocess
