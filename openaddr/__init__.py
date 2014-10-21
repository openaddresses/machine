from subprocess import Popen
from tempfile import mkdtemp
from os.path import realpath, join, basename, splitext, exists
from shutil import move, rmtree
from logging import getLogger

def conform(srcjson, destdir):
    ''' Python wrapper for openaddresses-conform.

        Generates all data in a temporary working
        directory, and does not use cached files.
    '''
    source, _ = splitext(basename(srcjson))
    workdir = mkdtemp(prefix='conform-')
    logger = getLogger('openaddr')

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

        logger.debug('openaddresses-conform {0} {1}'.format(srcjson, workdir))

        cmd = Popen(('node', index_js, srcjson, workdir), **cmd_args)
        cmd.wait()

        with open(join(destdir, source+'.status'), 'w') as file:
            file.write(str(cmd.returncode))

    logger.debug('{0} --> {1}'.format(source, workdir))

    #
    # Move resulting files to destination directory.
    #
    if exists(join(workdir, source+'.zip')):
        move(join(workdir, source+'.zip'), join(destdir, source+'.zip'))
        logger.debug(join(destdir, source+'.zip'))

    if exists(join(workdir, source, 'out.csv')):
        move(join(workdir, source, 'out.csv'), join(destdir, source+'.csv'))
        logger.debug(join(destdir, source+'.csv'))

    rmtree(workdir)
