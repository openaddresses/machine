''' Python wrapper for openaddresses-conform.

Looks in /var/opt/openaddresses-conform for an index.js to run. Generates
all data in a temporary working directory, and does not use cached files.
'''
from subprocess import Popen
from tempfile import mkdtemp
from os.path import realpath, join
from shutil import move, rmtree

source = 'us-ca-san_francisco'
source = 'us-ca-alameda_county'
srcjson = realpath('openaddresses/sources/{0}.json'.format(source))
workdir = mkdtemp(prefix='conform-')
destdir = 'tmp'

#
# Run openaddresses-conform from a fresh working directory.
#
# It tends to error silently and truncate data if it finds any existing
# data. Also, it wants to be able to write a file called ./tmp.csv.
#
errpath = join(destdir, source+'.stderr')
outpath = join(destdir, source+'.stdout')

with open(errpath, 'w') as stderr, open(outpath, 'w') as stdout:
    index_js = '/var/opt/openaddresses-conform/index.js'
    cmd_args = dict(cwd=workdir, stderr=stderr, stdout=stdout)

    print 'node', index_js, '...'

    cmd = Popen(('node', index_js, srcjson, workdir), **cmd_args)
    cmd.wait()

    with open(join(destdir, source+'.status'), 'w') as file:
        file.write(str(cmd.returncode))

print source, '-->', workdir

#
# Move resulting files to destination directory.
#
move(join(workdir, source+'.zip'), join(destdir, source+'.zip'))
print join(destdir, source+'.zip')

move(join(workdir, source, 'out.csv'), join(destdir, source+'.csv'))
print join(destdir, source+'.csv')

rmtree(workdir)
