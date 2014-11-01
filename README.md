OA Machine
==========

In-progress scripts for running OpenAddresses on a complete data set and publishing
the results. Uses [openaddresses](https://github.com/openaddresses/openaddresses),
[openaddresses-conform](https://github.com/openaddresses/openaddresses-conform),
and other components of OpenAddresses to work.

Status
------

Installation scripts for preparing a fresh install of Ubuntu 14.04 can be found
in `chef`. Run them from a Git checkout like this:

    sudo apt-get update
    sudo chef/run.sh

Complete sources will be checked out to `/var/opt/openaddresses/sources`.

OpenAddresses requires Amazon S3 to work. You can set the environment variables
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` or provide values as arguments
to `parallel.py`.

Test the complete process with `test.py`:

    python test.py

Run the complete process with `parallel.py`:

    python parallel.py -a <AWS key> -s <AWS secret> -l <log> openaddresses
