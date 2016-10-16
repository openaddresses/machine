OA Machine
==========

Scripts for running OpenAddresses on a complete data set and publishing
the results. Uses [OpenAddresses](https://github.com/openaddresses/openaddresses)
data sources to work.

Status
------

This code is being used to process the complete OA dataset on an expected-weekly
basis, with output visible at [data.openaddresses.io](http://data.openaddresses.io).

[![Build Status](https://travis-ci.org/openaddresses/machine.svg?branch=master)](https://travis-ci.org/openaddresses/machine/branches)

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
in `chef`. You will need a local installation of PostgreSQL, a PostgreSQL role 
named `dashboard`, a database named `openaddr` that's been initialized with the 
schema `openaddr/ci/schema.pgsql`, and an Amazon S3 bucket with credentials.

    createuser dashboard
    createdb openaddr
    ﻿psql -f openaddr/ci/schema.pgsql openaddr

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

### Batch Mode & CI Workers

To run batch mode with existing CI workers and queue, prepare a complete set of
sources from master branch with `openaddr-enqueue-sources`:

    openaddr-enqueue-sources -d <database URL> -t <Github token> -o <repo owner> -r <repo name>

Development
-----------

[Documentation for machine internals](docs/README.md) can help point you in the
right direction for development.

Test the OpenAddresses machine with `test.py`:

    python test.py

Run the webhook server, queue listener, and worker processes:

    python run-debug-webhooks.py
    python -m openaddr.ci.run_dequeue
    
    python -m openaddr.ci.worker
