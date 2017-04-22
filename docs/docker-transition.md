Docker Transition
===

_Michal Migurski, April 2017_

With OpenAddresses Machine 5.0.0, we’ve begun a transition from using Chef
to configure our EC2 servers to using Docker.

- Notes and ideas at [issue #562](https://github.com/openaddresses/machine/issues/562).
- Tests and work at [PR #587](https://github.com/openaddresses/machine/pull/587).

Conversations with a few developers suggest that the presence of a virtual
machine configuration such as Vagrant or Docker is a signal that code is easy
to approach and painless to get running. Chef has been effective for Machine,
but it’s difficult to approach and presents a challenge for developers who may
want to contribute work. We need to validate against the three main
environments where Machine code is run:

1. Locally on a developer’s Mac, Windows, or Linux computer with continuous edits to the code
2. In a CI environment like Travis or Circle CI for ensuring quality of new code changes
3. In production on AWS where we use Cloudwatch metrics, Auto Scaling Groups, and other features to maintain a running service which can respond to spikes in demand from OA contributors

In retrospect, the use of Docker on Travis and Circle was much easier than I had
assumed. The use of Docker for local development was instead much harder, and
in conversation with several developers I am under the impression that there’s
not really a universally-agreed best way to do this. It’s just kind of a mess.

Some observations:

-   It’s a bit simpler to run multiple workers per EC2 instance under Docker,
    so I’ve increased the number from one to two. This should result in lower
    overall costs and take advantage of our early observation that 2x the CPU
    count is a great worker count.
-   I’m using Docker host networking to ensure that Memcache is available at
    `127.0.0.1:11211` on the Webhooks instance and to save on some port-mapping
    overhead. A single EC2 AMI host OS supports all instance types, and
    includes pre-installed Apache with port 5000 proxied and Memcache in
    addition to Docker and AWS CLI.
-   We [use `uuid.getnode()`](https://github.com/openaddresses/machine/blob/4.x/openaddr/ci/__init__.py#L907-L908)
    to determine the identity of workers for the heartbeat queue that drives
    the active workers Cloudwatch metric. Under Docker, these seems to only
    result in two globally unique values, and we’ll probably want to switch to
    something else. It’s okay for now because this metric is only interesting
    for values of zero or non-zero, but it’s not quite correct.
-   Until all of our instance type groups are running under 5.x, the
    [instance cycling script](https://github.com/openaddresses/machine/blob/master/scripts/cycle-instances.sh)
    will be dangerous to run.
-   I’m running Docker as a plain bash background job with `&`, but maybe it
    would be smarter to stick with Procfiles under Honcho? If so, should that
    happen _inside_ the container or _outside_?

Most of our EC2 configuration takes place with user data shell scripts.
Here are new ones and old ones for comparison:

-   Worker user data 5.x with Docker:

        #!/bin/bash -ex
        docker pull openaddr/machine:5.x
        aws s3 cp s3://data.openaddresses.io/… /tmp/environment
        docker run --env-file /tmp/environment --volume /tmp:/tmp openaddr/machine:5.x openaddr-ci-worker -b data.openaddresses.io -v &
        docker run --env-file /tmp/environment --volume /tmp:/tmp openaddr/machine:5.x openaddr-ci-worker -b data.openaddresses.io -v &
        wait

-   Webhooks user data 5.x with Docker:

        #!/bin/bash -ex
        docker pull openaddr/machine:5.x
        aws s3 cp s3://data.openaddresses.io/… /tmp/environment
        docker run --env-file /tmp/environment --volume /tmp:/tmp --net="host" openaddr/machine:5.x gunicorn -w 4 --bind 127.0.0.1:5000 openaddr.ci.web:app &
        docker run --env-file /tmp/environment --volume /tmp:/tmp --net="host" openaddr/machine:5.x openaddr-ci-run-dequeue &
        wait

-   Worker user data 4.x with Chef:

        #!/bin/bash -ex

        # (Re)install machine.
        cd /home/ubuntu/machine
        sudo -u ubuntu git fetch origin 4.x
        sudo -u ubuntu git checkout FETCH_HEAD

        chef/run.sh prereqs
        aws s3 cp s3://data.openaddresses.io/… chef/data/local.json
        chef/run.sh worker

-   Webhooks user data 4.x with Chef:

        #!/bin/bash -ex

        # (Re)install machine.
        cd /home/ubuntu/machine
        sudo -u ubuntu git fetch origin 4.x
        sudo -u ubuntu git checkout FETCH_HEAD

        chef/run.sh prereqs
        aws s3 cp s3://data.openaddresses.io/… chef/data/local.json
        chef/run.sh webhooks