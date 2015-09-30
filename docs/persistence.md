Persistent Data
===============

Locations where we store data.

<a name="db">Database</a>
--------

The Machine database is a simple [PostgreSQL](http://www.postgresql.org) instance storing metadata about sources runs over time, such as timing, status, connection to batches, and links to results files on [S3](#s3).

Database tables:

1. Processing results of single sources, including sample data and output CSV’s, are added to the `runs` table.
2. Groups of `runs` resulting from Github events sent to [Webhook](components.md#webhook) are added to the `jobs` table.
3. Groups of `runs` periodically [enqueued as a batch](components.md#enqueue) are added to the `sets` table.

Other information:

* Complete schema can be [found in `openaddr/ci/schema.pgsql`](https://github.com/openaddresses/machine/blob/2.3.0/openaddr/ci/schema.pgsql).
* Public URL at [`machine-db.openaddresses.io`](postgres://machine-db.openaddresses.io).
* Lives on an [RDS `db.t2.micro` instance](https://console.aws.amazon.com/rds/home?region=us-east-1#dbinstances:id=machine;sf=all).
* Two weeks of nightly backups are kept.

<a name="q">Queue</a>
-----

The queue is used to schedule runs for [_Worker_ instances](components.md#worker), and its size is used to grow and shrink the _Worker_ pool. The queue is generally empty, and used only to store temporary data for scheduling runs. We use [PQ](https://github.com/malthe/pq) to implement the queue in Python. Data is stored in the one PostgreSQL database but treated as separate.

There are three queues:

1. `tasks` queue contains new runs to be handled.
2. `done` queue contains complete runs to be recognized.
3. `due` queue contains delayed runs that may have gone overtime.

Other information:

* Database [details are re-used](#db), with identical `machine-db.openaddresses.io` public URL.
* Queue [metrics in Cloudwatch](https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#metrics:metricFilter=Pattern%253Dopenaddr.ci) are kept up-to-date by [dequeuer](components.md#dequeue).
* Queue length [Cloudwatch alarms](https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#alarm:alarmFilter=ANY) determine [size of _Worker_ pool](components.md#worker).

<a name="s3">S3</a>
--

We use the S3 bucket `data.openaddresses.io` to store new and historical data.

* S3 access is handled via [the Boto library](http://docs.pythonboto.org/en/latest/).
* Boto expects current AWS credentials in the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables.

<a name="mapbox">Mapbox</a>
------

We use the Mapbox API account `open-addresses` to store a tiled dot map with worldwide locations of address points.

* Uploads are handled via [the Boto3 library](https://boto3.readthedocs.org), using credentials granted by the Mapbox API.
