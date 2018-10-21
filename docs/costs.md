Costs
=====

Machine’s [components](components.md) and [processes](processes.md) can be
tuned to raise or lower costs and efficiency based on tradeoffs and goals.

- Adjust frequencies of expensive scheduled tasks
- Modify run-reuse timeout for previously-calculated results
- Change types and workloads of worker instances
- Change types and numbers of webhook instances

### Scheduled Tasks

Large tasks that use the entire OpenAddresses dataset are [scheduled with AWS Cloudwatch events](http://docs.aws.amazon.com/AmazonCloudWatch/latest/events/WhatIsCloudWatchEvents.html).
Event rules are updated with details found [in `update-scheduled-tasks.py`](https://github.com/openaddresses/machine/blob/6.8.9/ops/update-scheduled-tasks.py),
and typically trigger task-specific, single-use EC2 instances via AWS Lambda
code found [in `run-ec2-command.py`](https://github.com/openaddresses/machine/blob/6.8.9/ops/run-ec2-command.py).

Tasks:

- [Calculate Coverage](https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#rules:name=OA-Calculate-Coverage)
- [Collect Extracts](https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#rules:name=OA-Collect-Extracts)
- [Enqueue Sources](https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#rules:name=OA-Enqueue-Sources)
- [Index Tiles](https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#rules:name=OA-Index-Tiles)
- [Update Dotmap](https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#rules:name=OA-Update-Dotmap)

Each task has a variable run frequency, instance type, and time limit.

### Worker

Does the actual work of running a source and producing output files.

Workers are defined in the [CI Workers 6.x AutoScaling Group](https://console.aws.amazon.com/ec2/autoscaling/home?region=us-east-1#AutoScalingGroups:id=CI+Workers+6.x;view=instances),
with a variable target count of `m3.medium` instances. When there are new jobs
available via the queue, workers are added. After a quiet period, they are
terminated.

Each worker instance has two parallel worker processes, set in the
[CI Workers 6.x Launch Configuration](https://console.aws.amazon.com/ec2/autoscaling/home?region=us-east-1#LaunchConfigurations:id=CI+Workers+6.x+(4)):

    honcho -f /usr/local/src/openaddr/ops/Procfile-worker start -c worker=2

Cached results of previous runs can be re-used, as long as they are within the
defined [`RUN_REUSE_TIMEOUT` time period](https://github.com/openaddresses/machine/blob/6.8.9/openaddr/ci/__init__.py#L63-L64)
currently defined as 10 days.

### Webhook

This [Python + Flask](http://flask.pocoo.org) application is the center of the
OpenAddresses Machine. _Webhook_ maintains a connection to the
[database](persistence.md#db) and [queue](#q), listens for new CI jobs from
[Github event hooks](https://developer.github.com/webhooks/#events) on the
[OpenAddresses repository](https://github.com/openaddresses/openaddresses),
queues new source runs, and displays results of batch sets over time.

It’s defined in the [CI Webhooks 6.x AutoScaling Group](https://console.aws.amazon.com/ec2/autoscaling/home?region=us-east-1#AutoScalingGroups:id=CI+Webhooks+6.x;view=details),
with a target instance count of one `t2.small` EC2 instance running a Gunicorn
process with multiple workers.
