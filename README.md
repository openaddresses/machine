OA Machine
==========

In-progress scripts for running OpenAddresses on a complete data set and publishing
the results. Uses [openaddresses](https://github.com/openaddresses/openaddresses),
[openaddresses-conform](https://github.com/openaddresses/openaddresses-conform),
and other components of OpenAddresses to work.

Status
------

This code is being used to process the complete OA dataset on an expected-weekly
basis, with output visible at [data.openaddresses.io](http://data.openaddresses.io).

Installation scripts for preparing a fresh install of Ubuntu 14.04 can be found
in `chef`. Run them from a Git checkout like this:

    sudo apt-get update
    sudo chef/run.sh

Complete sources will be checked out to `/var/opt/openaddresses/sources`.

Run a single source locally with `openaddr-process-one`:

    openaddr-process-one -l <log> <path to source JSON> <output directory>

For more than one source file, OpenAddresses requires Amazon S3 to work.
You can set the environment variables `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY` or provide values as arguments to `openaddr-process`.

Run the complete process with `openaddr-process`:

    openaddr-process -a <AWS key> -s <AWS secret> -l <log> data.openaddresses.io

Run it on an Amazon EC2 spot instance with `openaddr-ec2-run`:

    openaddr-ec2-run -a <AWS key> -s <AWS secret> data.openaddresses.io

Development
-----------

Modify the contents of [`openaddr/paths.py`](openaddr/paths.py) with locations
of your local [openaddresses](https://github.com/openaddresses/openaddresses),
[openaddresses-conform](https://github.com/openaddresses/openaddresses-conform),
[openaddresses-cache](https://github.com/openaddresses/openaddresses-cache),
and other components of OpenAddresses.

Test the OpenAddresses machine with `test.py`:

    python test.py

Run the complete process from the `openaddr` module:

    python -m openaddr.process_all -a <AWS key> -s <AWS secret> -l <log> data.openaddresses.io
