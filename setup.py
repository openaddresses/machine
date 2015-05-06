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
    packages = ['openaddr', 'openaddr.ci', 'openaddr.tests'],
    package_data = {
        'openaddr': [
            'VERSION'
        ],
        'openaddr.ci': [
            'schema.pgsql'
        ]
    },
    install_requires = [
        'Flask == 0.10.1', 'gunicorn == 19.3.0', 'httmock == 1.2.3',
        'itsdangerous == 0.24', 'Jinja2 == 2.7.3', 'MarkupSafe == 0.23',
        'mock == 1.0.1', 'pq == 1.2', 'psycopg2 == 2.6', 'simplejson == 3.6.5',
        'uritemplate == 0.6', 'Werkzeug == 0.10.4',
        
        'boto >= 2.22.0', 'Jinja2 >= 2.7.0', 'dateutils >= 0.6', 'ijson >= 2.0',

        # https://bugs.launchpad.net/ubuntu/+source/python-pip/+bug/1306991/comments/10
        'requests == 2.2.1',

        # https://pypi.python.org/pypi/requests-ftp, appears no longer maintained.
        'requests-ftp == 0.2.0',

        # https://github.com/patrys/httmock
        'httmock >= 1.2',

        # https://pypi.python.org/pypi/setproctitle/
        'setproctitle >= 1.1.8'

        ] + conditional_requirements,
    entry_points = dict(
        console_scripts = [
            'openaddr-ci-recreate-db = openaddr.ci.recreate_db:main',
            'openaddr-ci-run-dequeue = openaddr.ci.run_dequeue:main',
            'openaddr-ci-worker = openaddr.ci.worker:main',
        ]
    )
)
