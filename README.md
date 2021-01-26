<h1 align="center">OA Machine [Deprecated]</h1>


Legacy scripts for running OpenAddresses on a complete data set and publishing
the results. Uses [OpenAddresses](https://github.com/openaddresses/openaddresses)
data sources to work.



Status
------

README: This code powers the legacy https://results.openaddresses.io/ site. Current development efforts should be focused on the batch service
at https://batch.openaddresses.io/data. This service is powered by a fork of the machine code found https://github.com/openaddresses/batch-machine. Changes made to this code **will not** affect the newer, batch service.

This code is being used to process the complete OA dataset on a weekly and on-demand
basis, with output visible at [results.openaddresses.io](https://results.openaddresses.io).

[![Build Status](https://travis-ci.org/openaddresses/machine.svg?branch=master)](https://travis-ci.org/openaddresses/machine/branches)

Usage
-----
NOTE: machine's use for CI has been deprecated and it is currenty serving a static version of sources processed up to August, 2020.
All CI and weekly source functionality is disabled. For current data please see the [batch service](https://batch.openaddresses.io)

Machine is an integral of the OpenAddresses project. When new sources
[are added in Github](https://github.com/openaddresses/openaddresses#contributing-addresses),
they are automatically processed and status output is displayed in Github’s
pull request UI. A successful set of checks looks like this:

![Github status display](docs/github-status.png)

More information about Machine’s output can be seen by following the Details link
[to a job page like this](http://results.openaddresses.io/jobs/b044ce9c-caa0-46fb-a7e4-842beeae3f52).

Machine also runs its own weekly batch process to generate the downloadable
files, maps, and other resources available via [results.openaddresses.io](https://results.openaddresses.io).

![OpenAddresses worldwide coverage map](https://data.openaddresses.io/render-world.png)

Development
-----------

[Documentation for Machine internals](https://docs.contour.so/openaddresses/machine) can help point you in the
right direction for development. Follow the [installation instructions](https://docs.contour.so/openaddresses/machine/manual-2oq7vh5jlns0000000000)
to use and modify Machine code locally.
