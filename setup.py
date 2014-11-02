from setuptools import setup

setup(
    name = 'OpenAddresses',
    version = '0.1.0',
    packages = ['openaddr'],
    package_data = {
        'openaddr': ['geodata/*.shp', 'geodata/*.shx', 'geodata/*.prj', 'geodata/*.dbf', 'geodata/*.cpg']
    }
)
