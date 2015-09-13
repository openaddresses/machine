Processes
=========

Periodic and event-driven processes paths through components and persistent data stores.

<a name="set">Batch Set</a>
---------

Batch sets are used approximately once per week, scheduled with `cron`.

1.  Run the [batch enqueue with the script `openaddr-enqueue-sources`](components.md#enqueue).
    This will require a current [Github access token](https://help.github.com/articles/creating-an-access-token-for-command-line-use/)
    and a connection to [the machine database](persistence.md#db):
    
        openaddr-enqueue-sources -t <Github Token> -d <Database URL>
    
2.  Complete sources are read from Github’s API using the current master branch
    of the [OpenAddresses repository](https://github.com/openaddresses/openaddresses).
    
3.  A new empty set is created in the [`sets` table](persistence.md#db), and
    becomes visible at [results.openaddresses.io/sets](http://results.openaddresses.io/sets).
    
4.  New runs are slowly drip-fed into the [`tasks` queue](persistence.md#queue).
    New items are only enqueued when the queue length is zero, to prevent
    [_Worker_ auto-scale costs](components.md#worker) from ballooning.
    
5.  [_Worker_ processes runs](components.md#worker) from the queue, storing
    results in [S3](persistence.md#s3) and passing completed runs to the
    [`done` queue](persistence.md#queue).
    
6.  Completed run information is [handled by _Dequeuer_](components.md#dequeue).
    
7.  When all runs are finished, new coverage maps are rendered and
    `openaddr-enqueue-sources` exits successfully.

<a name="job">CI Job</a>
------

Continuous integration jobs are used each time an OpenAddresses contributor
modifies the [main repository](https://github.com/openaddresses/openaddresses)
with a pull request.

1.  A contributor [issues a pull request](https://help.github.com/articles/using-pull-requests/).
    
2.  Github posts a blob of JSON data describing the edits to
    [_Webhook_ `/hook` endpoint](components.md#webhook).
    
3.  _Webhook_ immediately attempts to create a new empty job in the
    [`jobs` table](persistence.md#db) and enqueues any new source runs found in
    the edits.
    
    If this step fails, an _error_ status is posted back to the
    [Github status API](https://developer.github.com/v3/repos/statuses/), and
    no job or run is created.
    
    If this step succeeds, a _pending_ status is posted back to the Github
    status API, and the job becomes visible at
    [results.openaddresses.io/jobs](http://results.openaddresses.io/jobs).
    
4.  [_Worker_ processes runs](components.md#worker) from the queue, storing
    results in [S3](persistence.md#s3) and passing completed runs to the
    [`done` queue](persistence.md#queue).
    
5.  Completed run information is [handled by _Dequeuer_](components.md#dequeue).
    
6.  When all runs are finished, a final _success_ or _failure_ status is posted
    back to the Github status API.

Collection
----------

New Zip collections are generated nightly, scheduled with `cron`.

1.  Run the [collection with the script `openaddr-collect-extracts`](components.md#collect).
    This will require a connection to [the machine database](persistence.md#db)
    and [S3 access credentials](persistence.md#s3) in environment variables:
    
        openaddr-collect-extracts -d <Database URL>
    
2.  Current data is read from the [`sets` and `runs` tables](persistence.md#db),
    using the most-recent successful run for each source listed in the most
    recent set. This will include older successful runs for sources that have
    since failed.
    
3.  New Zip archives are created for geographic regions of the world.
    
4.  Zip archives are [uploaded to S3](persistence.md#s3) in predictable locations
    overwriting previous archives, and immediately available from
    [results.openaddresses.io](http://results.openaddresses.io).
