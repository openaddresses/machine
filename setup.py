from setuptools import setup
from os.path import join, dirname
import sys

with open(join(dirname(__file__), 'openaddr', 'VERSION')) as file:
    version = file.read().strip()

conditional_requirements = list()

if sys.version_info[0] == 2:
    conditional_requirements += [
        # http://python-future.org
        'future >= 0.14.3',
        
        # https://github.com/jdunck/python-unicodecsv
        'unicodecsv >= 0.11.2',
        
        # https://code.google.com/p/python-subprocess32/
        'subprocess32 == 3.2.6',

        # https://pypi.python.org/pypi/python-memcached
        'python-memcached == 1.57',
    ]
else:
    conditional_requirements += [
        # https://pypi.python.org/pypi/python-memcached
        'python3-memcached == 1.51',
    ]

setup(
    name = 'OpenAddresses-Machine',
    version = version,
    url = 'https://github.com/openaddresses/machine',
    author = 'Michal Migurski',
    author_email = 'mike-pypi@teczno.com',
    description = 'In-progress scripts for running OpenAddresses on a complete data set and publishing the results.',
    packages = ['openaddr', 'openaddr.util', 'openaddr.ci', 'openaddr.tests'],
    entry_points = dict(
        console_scripts = [
            'openaddr-render-us = openaddr.render:main',
            'openaddr-process-one = openaddr.process_one:main',
            'openaddr-esri2geojson = openaddr.util.esri2geojson:main',
            'openaddr-ci-recreate-db = openaddr.ci.recreate_db:main',
            'openaddr-ci-run-dequeue = openaddr.ci.run_dequeue:main',
            'openaddr-ci-worker = openaddr.ci.worker:main',
            'openaddr-enqueue-sources = openaddr.ci.enqueue:main',
            'openaddr-collect-extracts = openaddr.ci.collect:main',
        ]
    ),
    package_data = {
        'openaddr': [
            'geodata/*.shp', 'geodata/*.shx', 'geodata/*.prj', 'geodata/*.dbf',
            'geodata/*.cpg', 'templates/*.*', 'VERSION',
        ],
        'openaddr.ci': [
            'schema.pgsql', 'templates/*.*', 'static/*.*'
        ],
        'openaddr.tests': [
            'data/*.*', 'sources/*.*', 'sources/fr/*.*', 'conforms/*.*'
        ]
    },
    test_suite = 'openaddr.tests',
    install_requires = [
        'boto >= 2.22.0', 'Jinja2 >= 2.7.0', 'dateutils >= 0.6', 'ijson >= 2.0',

        # http://flask.pocoo.org
        'Flask == 0.10.1',
        
        # http://gunicorn.org
        'gunicorn == 19.3.0',

        # http://www.voidspace.org.uk/python/mock/
        'mock == 1.0.1',

        # https://github.com/uri-templates/uritemplate-py/
        'uritemplate == 0.6',
        
        # https://github.com/malthe/pq/
        'pq == 1.3',
        
        # http://initd.org/psycopg/
        'psycopg2 == 2.6.1',
        
        # https://bugs.launchpad.net/ubuntu/+source/python-pip/+bug/1306991/comments/10
        'requests == 2.2.1',

        # https://pypi.python.org/pypi/requests-ftp, appears no longer maintained.
        'requests-ftp == 0.2.0',

        # https://github.com/patrys/httmock
        'httmock >= 1.2',
        
        # https://boto3.readthedocs.org
        'boto3 == 1.1.4',

        ] + conditional_requirements
)
