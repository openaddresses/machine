### Installation

* Tested on Ubuntu 14.04 server. 16GB RAM.

```
# sudo apt-get install libgdal-dev python3-pip libffi-dev python3-cairo python3-gdal
# sudo pip3 install virtualenv
# python3 -m virtualenv ./env && source ./env/bin/activate
# pip3 install fiona shapely requests cairocffi

# sudo apt-add-repository ppa:ubuntugis/ubuntugis-unstable
# sudo apt-get update
# pip3 install --global-option=build_ext --global-option="-I/usr/include/gdal" GDAL==`gdal-config --version


# git clone https://github.com/openaddresses/machine
# cd machine && pip install .

# git clone https://github.com/openaddresses/openaddresses.git  # inside of the parcels repo
# python3 parse.py

```
