Components
==========

<a name="webhook">Webhook</a>
-------

This [Python + Flask](http://flask.pocoo.org) application is the center of the OpenAddresses Machine. _Webhook_ maintains a connection to the [database](#db) and [queue](#q), listens for new CI jobs from [Github event hooks](https://developer.github.com/webhooks/#events) on the [OpenAddresses repository](https://github.com/openaddresses/openaddresses), queues new source runs, and displays results of batch sets over time.

* Run [from a Procfile using gunicorn](https://github.com/openaddresses/machine/blob/2.1.8/chef/Procfile-webhook#L1).
* Flask code can be [found in `openaddr/ci/webhooks.py`](https://github.com/openaddresses/machine/blob/2.1.8/openaddr/ci/webhooks.py).
* Public URL at [`results.openaddresses.io`](http://results.openaddresses.io).
* Lives on a long-running, 24×7 [EC2 `t2.small` instance](https://console.aws.amazon.com/ec2/v2/home?region=us-east-1#Instances:instanceId=i-bdacc315;sort=Name).

<a name="worker">Worker</a>
------

This Python script accepts new source runs from the [`tasks` queue](#queue), converts them into output Zip archives with CSV files, uploads those to [S3](#s3), and notifies the [dequeuer](#dequeuer) via the [`due` and `done` queues](#queue). _Worker_ is intended to be run in parallel, and uses EC2 auto-scaling to respond to increased demand by launching new instances. One worker is kept alive at all times on the same EC2 instance as _Webhook_.

The actual work is done a separate sub-process, [using the `openaddr-process-one` script](https://github.com/openaddresses/machine/blob/2.1.8/setup.py#L41).

* Run [from a Procfile](https://github.com/openaddresses/machine/blob/2.1.8/chef/Procfile-worker).
* _Worker_ code can be found [in `openaddr/ci/worker.py`](https://github.com/openaddresses/machine/blob/2.1.8/openaddr/ci/worker.py).
* `openaddr-process-one` code can be found [in `openaddr/process_one.py`](https://github.com/openaddresses/machine/blob/2.1.8/openaddr/process_one.py).
* Configured in an [EC2 auto-scaling group]( https://console.aws.amazon.com/ec2/autoscaling/home?region=us-east-1#AutoScalingGroups:id=CI+Workers+2.x;view=details) with [launch configuration]( https://console.aws.amazon.com/ec2/autoscaling/home?region=us-east-1#LaunchConfigurations:id=CI+Workers+2.x).
* The time allotted for a single source run is [currently limited to 3 hours](https://github.com/openaddresses/machine/blob/2.1.8/openaddr/jobs.py#L29).
* No public URLs.

<a name="dequeue">Dequeuer</a>
--------

This Python script watches the [`done` and `due` queues](#queue). Run status is updated based on the contents of those queues: if a run appears in the `due` queue first, it will be marked as failed and any subsequent `done` queue item will be ignored. If a run appears in the `done` queue first, it will be marked as successful. Statuses are [posted to the Github status API](https://developer.github.com/v3/repos/statuses/) for runs connected to a CI job initiated by _Webhook_ and [to the `runs` table](#db) with links.

This script also watches the overall size of the queue, and [updates Cloudwatch metrics](https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#metrics:metricFilter=Pattern%253Dopenaddr.ci) to determine when [the _Worker_ pool](#worker) needs to grow or shrink.

* Run [from a Procfile](https://github.com/openaddresses/machine/blob/2.1.8/chef/Procfile-webhook#L2), on the same EC2 instance as _Webhook_.
* _Dequeue_ code can be found [in `openaddr/ci/run_dequeue.py`](https://github.com/openaddresses/machine/blob/2.1.8/openaddr/ci/run_dequeue.py).
* No public URL.

Scheduled Tasks
---------------

### <a name="enqueue">Batch Enqueue</a>



### Collect



<a name="db">Database</a>
--------

The Machine database is a simple [PostgreSQL](http://www.postgresql.org) instance storing metadata about sources runs over time, such as timing, status, connection to batches, and links to results files on [S3](#s3).

Database tables:

1. Processing results of single sources, including sample data and output CSV’s, are added to the `runs` table.
2. Groups of `runs` resulting from Github events sent to [Webhook](#webhook) are added to the `jobs` table.
3. Groups of `runs` periodically [enqueued as a batch](#enqueue) are added to the `sets` table.

Other information:

* Complete schema can be [found in `openaddr/ci/schema.pgsql`](https://github.com/openaddresses/machine/blob/2.1.8/openaddr/ci/schema.pgsql).
* Public URL at [`machine-db.openaddresses.io`](postgres://machine-db.openaddresses.io).
* Lives on an [RDS `db.t2.micro` instance](https://console.aws.amazon.com/rds/home?region=us-east-1#dbinstances:id=machine;sf=all).

<a name="q">Queue</a>
-----



<a name="s3">S3</a>
-----
