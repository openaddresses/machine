OA Machine
==========

Scripts for running OpenAddresses on a complete data set and publishing
the results. Uses [OpenAddresses](https://github.com/openaddresses/openaddresses)
data sources to work.

Status
------

This code is being used to process the complete OA dataset on an expected-weekly
basis, with output visible at [data.openaddresses.io](http://data.openaddresses.io).

[![Build Status](https://travis-ci.org/openaddresses/machine.svg?branch=master)](https://travis-ci.org/openaddresses/machine)

Usage
-----

Machine supports two modes. Continuous integration (CI) mode listens for tasks
from the OpenAddresses Github repository, includes a persistent web server and
database, and supports a flexible number of worker processes to deal with large
changes quickly. Batch mode processes the entire OpenAddresses source
collection at once, and is intended to be run periodically to reflect new
submissions.

### CI Mode

Installation scripts for preparing a fresh install of Ubuntu 14.04 can be found
in `chef`. You will need a pre-installed PostgreSQL database initialized with
the schema `openaddr/ci/schema.pgsql` and Amazon S3 bucket with credentials.
After editing `chef/role-webhooks.json` and `chef/role-worker.json`, run them
from a Git checkout like this:

    sudo apt-get update
    sudo chef/run.sh webhooks
    sudo chef/run.sh worker

`webhooks` will install Apache, a web application to listen for new tasks, and
a small queue observer to watch for completed tasks. `worker` will install the
`openaddr-ci-worker` utility to watch for newly-scheduled tasks.

Run a single source locally with `openaddr-process-one`:

    openaddr-process-one -l <log> <path to source JSON> <output directory>

### Batch Mode

Installation scripts for preparing a fresh install of Ubuntu 14.04 can be found
in `chef`. Run them from a Git checkout like this:

    sudo apt-get update
    sudo chef/run.sh batchmode

Complete sources will be checked out to `/var/opt/openaddresses/sources`.

Run a single source locally with `openaddr-process-one`:

    openaddr-process-one -l <log> <path to source JSON> <output directory>

For more than one source file, OpenAddresses requires Amazon S3 to work.
You can set the environment variables `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY` or provide values as arguments to `openaddr-process`.

Run the complete process with `openaddr-process`:

    openaddr-process -a <AWS key> -s <AWS secret> -l <log> <path to sources dir> data.openaddresses.io

Run it on an Amazon EC2 spot instance with `openaddr-ec2-run`:

    openaddr-ec2-run -a <AWS key> -s <AWS secret> data.openaddresses.io

### Batch Mode & CI Workers

To run batch mode with existing CI workers and queue, prepare a complete set of
sources from master branch with `openaddr-ci-enqueue`:

    openaddr-ci-enqueue -d <database URL> -t <Github token> -o <repo owner> -r <repo name>

Development
-----------

Test the OpenAddresses machine with `test.py`:

    python test.py

Run the webhook server, queue listener, and worker processes:

    python run-debug-webhooks.py
    python -m openaddr.ci.run_dequeue
    
    python -m openaddr.ci.worker

Modify the contents of [`openaddr/paths.py`](openaddr/paths.py) with locations
of your local [openaddresses](https://github.com/openaddresses/openaddresses).

Run the complete batch process from the `openaddr` module:

    python -m openaddr.process_all -a <AWS key> -s <AWS secret> -l <log> data.openaddresses.io

Extras
------

Convert remote ESRI feature services to GeoJSON with `openaddr-esri2geojson`:

    openaddr-esri2geojson <ESRI URL> <GeoJSON path>
