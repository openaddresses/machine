from setuptools import setup
from os.path import join, dirname

with open(join(dirname(__file__), 'openaddr', 'VERSION')) as file:
    version = file.read().strip()

setup(
    name = 'OpenAddresses-Machine',
    version = version,
    url = 'https://github.com/openaddresses/machine',
    author = 'Michal Migurski',
    author_email = 'mike-pypi@teczno.com',
    description = 'In-progress scripts for running OpenAddresses on a complete data set and publishing the results.',
    packages = ['openaddr', 'openaddr.util', 'openaddr.ci', 'openaddr.ci.coverage', 'openaddr.tests', 'openaddr.parcels'],
    entry_points = dict(
        console_scripts = [
            'openaddr-render-us = openaddr.render:main',
            'openaddr-preview-source = openaddr.preview:main',
            'openaddr-process-one = openaddr.process_one:main',
            'openaddr-ci-recreate-db = openaddr.ci.recreate_db:main',
            'openaddr-ci-run-dequeue = openaddr.ci.run_dequeue:main',
            'openaddr-ci-worker = openaddr.ci.worker:main',
            'openaddr-enqueue-sources = openaddr.ci.enqueue:main',
            'openaddr-collect-extracts = openaddr.ci.collect:main',
            'openaddr-index-tiles = openaddr.ci.tileindex:main',
            'openaddr-update-dotmap = openaddr.dotmap:main',
            'openaddr-sum-up-data = openaddr.ci.sum_up:main',
            'openaddr-calculate-coverage = openaddr.ci.coverage.calculate:main',
        ]
    ),
    package_data = {
        'openaddr': [
            'geodata/*.shp', 'geodata/*.shx', 'geodata/*.prj', 'geodata/*.dbf',
            'geodata/*.cpg', 'VERSION',
        ],
        'openaddr.ci': [
            'schema.pgsql', 'templates/*.*', 'static/*.*'
        ],
        'openaddr.ci.coverage': [
            'schema.pgsql'
        ],
        'openaddr.tests': [
            'data/*.*', 'outputs/*.*', 'sources/*.*', 'sources/fr/*.*',
            'sources/us/*/*.*', 'sources/de/*.*', 'sources/nl/*.*',
            'sources/be/*/*.json', 'conforms/lake-man-gdb.gdb/*',
            'conforms/*.csv', 'conforms/*.dbf', 'conforms/*.zip', 'conforms/*.gfs',
            'conforms/*.gml', 'conforms/*.json', 'conforms/*.prj', 'conforms/*.shp',
            'conforms/*.shx', 'conforms/*.vrt',
            'parcels/sources/us/ca/*.*', 'parcels/sources/us/id/*.*',
            'parcels/data/*.*', 'parcels/data/us/ca/*.*',
            'parcels/data/us/ca/berkeley/*.*'
        ],
        'openaddr.parcels': [
            'README.md'
        ],
        'openaddr.util': [
            'templates/*.*'
        ]
    },
    test_suite = 'openaddr.tests',
    install_requires = [
        'boto == 2.43.0', 'dateutils == 0.6.6', 'ijson == 2.3',
        
        # Honcho (imported for worker) requires Jinja2 < 2.8.
        'Jinja2 == 2.7.3',

        # http://flask.pocoo.org
        'Flask == 0.12.3',
        
        # http://flask-cors.corydolphin.com
        'Flask-Cors == 3.0.2',
        
        # https://www.palletsproject.com/projects/werkzeug/
        'Werkzeug == 0.11.11',
        
        # http://gunicorn.org
        'gunicorn == 19.6.0',

        # http://www.voidspace.org.uk/python/mock/
        'mock == 2.0.0',

        # https://github.com/uri-templates/uritemplate-py/
        'uritemplate == 0.6',
        
        # https://github.com/malthe/pq/
        'pq == 1.4',
        
        # http://initd.org/psycopg/
        'psycopg2 == 2.7.3.2',
        
        # http://docs.python-requests.org/en/master/
        'requests == 2.11.1',

        # https://github.com/patrys/httmock
        'httmock == 1.2.5',
        
        # https://boto3.readthedocs.org
        'boto3 == 1.4.4',

        # https://github.com/openaddresses/pyesridump
        'esridump == 1.6.0',

        # Used in openaddr.parcels
        'Shapely == 1.5.17',
        'Fiona == 1.7.0.post2',

        # http://pythonhosted.org/itsdangerous/
        'itsdangerous == 0.24',

        # https://pypi.python.org/pypi/python-memcached
        'python-memcached == 1.59',

        # https://github.com/tilezen/mapbox-vector-tile
        'mapbox-vector-tile==1.2.0',
        'future==0.16.0',
        'protobuf==3.5.1',
        'pyclipper==1.1.0',
        'six==1.11.0',

        ]
)
