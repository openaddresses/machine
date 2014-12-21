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
    packages = ['openaddr'],
    package_data = {
        'openaddr': [
            'geodata/*.shp', 'geodata/*.shx', 'geodata/*.prj', 'geodata/*.dbf',
            'geodata/*.cpg', 'templates/*.html', 'templates/*.sh', 'VERSION'
        ]
    },
    install_requires = [
        'boto >= 2.22.0', 'Jinja2 >= 2.7.0', 'dateutils >= 0.6', 'ijson >= 2.0',
        
        # https://bugs.launchpad.net/ubuntu/+source/python-pip/+bug/1306991/comments/10
        'requests==2.2.1'
        ],
    entry_points = dict(
        console_scripts = [
            'openaddr-render-us = openaddr.render:main',
            'openaddr-summarize = openaddr.summarize:main',
            'openaddr-process = openaddr.process:main',
            'openaddr-process-one = openaddr.process2:main',
            'openaddr-ec2-run = openaddr.run:main'
        ]
    )
)
