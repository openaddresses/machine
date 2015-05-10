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
    ]

setup(
    name = 'OpenAddresses-Machine',
    version = version,
    url = 'https://github.com/openaddresses/machine',
    author = 'Michal Migurski',
    author_email = 'mike-pypi@teczno.com',
    description = 'In-progress scripts for running OpenAddresses on a complete data set and publishing the results.',
    entry_points = dict(
        console_scripts = [
            'openaddr-ci-recreate-db = openaddr.ci.recreate_db:main',
            'openaddr-ci-run-dequeue = openaddr.ci.run_dequeue:main',
            'openaddr-ci-worker = openaddr.ci.worker:main',
        ]
    ),
    packages = ['openaddr', 'openaddr.ci', 'openaddr.tests'],
    package_data = {
        'openaddr': [
            'VERSION'
        ],
        'openaddr.ci': [
            'schema.pgsql'
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
        'pq == 1.2',
        
        # http://initd.org/psycopg/
        'psycopg2 == 2.6',
        
        # https://bugs.launchpad.net/ubuntu/+source/python-pip/+bug/1306991/comments/10
        'requests == 2.2.1',

        # https://pypi.python.org/pypi/requests-ftp, appears no longer maintained.
        'requests-ftp == 0.2.0',

        # https://github.com/patrys/httmock
        'httmock >= 1.2',

        # https://pypi.python.org/pypi/setproctitle/
        'setproctitle >= 1.1.8'

        ] + conditional_requirements
)
