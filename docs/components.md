Components
==========

<a name="webhook">Webhook</a>
-------

This [Python + Flask](http://flask.pocoo.org) application is the center of the OpenAddresses Machine. _Webhook_ maintains a connection to the [database](#db) and [queue](#q), listens for new CI jobs from [Github event hooks](https://developer.github.com/webhooks/#events) on the [OpenAddresses repository](https://github.com/openaddresses/openaddresses), queues new source runs, and displays results of batch sets over time.

* Run [from a Procfile using gunicorn](https://github.com/openaddresses/machine/blob/2.1.8/chef/Procfile-webhook#L1).
* Flask code can be [found in `openaddr/ci/webhooks.py`](https://github.com/openaddresses/machine/blob/2.1.8/openaddr/ci/webhooks.py).
* Public URL at [`results.openaddresses.io`](http://results.openaddresses.io).
* Lives on a long-running, 24×7 [EC2 `t2.small` instance](https://console.aws.amazon.com/ec2/v2/home?region=us-east-1#Instances:instanceId=i-bdacc315;sort=Name).

Worker
------



Dequeuer
--------



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
