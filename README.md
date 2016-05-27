### Installation

* Tested on Ubuntu 14.04 server. 16GB RAM.

```
# sudo apt-get install libgdal-dev python3-pip libffi-dev python3-cairo python3-gdal
# python3 -m virtualenv ./env && source ./env/bin/activate
# pip3 install -r requirements.txt

# git clone https://github.com/openaddresses/machine
# cd machine && pip install .

# git clone https://github.com/openaddresses/openaddresses.git

```
Update `config.py` with the directory of the openaddresses repo.

```
# python3 parse.py
```

*Note* - gdal was a pain to install on osx and ubuntu, here are some steps I needed to take.

```
# sudo apt-add-repository ppa:ubuntugis/ubuntugis-unstable
# sudo apt-get update
# pip3 install --global-option=build_ext --global-option="-I/usr/include/gdal" GDAL==`gdal-config --version

```
