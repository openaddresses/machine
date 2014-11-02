from setuptools import setup

setup(
    name = 'OpenAddresses',
    version = '0.1.0',
    packages = ['openaddr'],
    package_data = {
        'openaddr': [
            'geodata/*.shp', 'geodata/*.shx', 'geodata/*.prj', 'geodata/*.dbf',
            'geodata/*.cpg', 'templates/*.html'
        ]
    },
    entry_points = dict(
        console_scripts = [
            'openaddr-render-us = openaddr.render:main',
            'openaddr-summarize = openaddr.summarize:main'
        ]
    )
)
