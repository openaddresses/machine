Install
=======

This document describes how to install the Machine code for local development, and demonstrates two ways to use it: running a single source and running a complete batch set. If you’re editing a lot of sources and want to do it quickly without waiting for a remote Github-based continuous integration service, you may want to use run single sources locally. If you're working on the queuing and job control portions of Machine code, you may want to run complete batch sets on test data.

Running A Source Locally
------------------------

Run a single source without installing Python or other packages locally
using [OpenAddresses from Docker Hub](https://hub.docker.com/r/openaddr/).

1.  Get the latest [OpenAddresses image from Docker Hub](https://hub.docker.com/r/openaddr/machine/tags/):
    
        docker pull openaddr/machine:6.x

2.  Download a source from [OpenAdresses/openaddresses on Github](https://github.com/openaddresses/openaddresses). [Berkeley, California](https://results.openaddresses.io/sources/us/ca/berkeley) is a small, reliable source that’s good to test with:

        curl -o us-ca-berkeley.json \
          -L https://github.com/openaddresses/openaddresses/raw/master/sources/us/ca/berkeley.json

3.  Using Docker, run `openaddr-process-one` to process the source:

        docker run --volume `pwd`:/vol openaddr/machine \
          openaddr-process-one -v vol/us-ca-berkeley.json vol

4.  Look in the directory `us-ca-berkeley` for address output, logs, and other files.

Local Development
-----------------

You can edit a local copy of OpenAddresses code with working tests by installing
everything onto a local virtual machine using [Docker](https://www.docker.com).
This process should take 5-10 minutes depending on download speed.

1.  Download and install [Docker](https://www.docker.com). On Mac OS X,
    use [Docker for Mac](https://docs.docker.com/docker-for-mac/). On Ubuntu,
    run `apt-get install docker.io` or follow [Docker’s own directions](https://docs.docker.com/engine/installation/linux/ubuntu/).

2.  Build the required image, which includes binary packages like GDAL and Postgres.
    
        VERSION=`cut -f1 -d. openaddr/VERSION`.x
        docker build -f Dockerfile-machine -t openaddr/machine:$VERSION .
    
3.  Run everything in detached mode:
    
        docker-compose up -d
    
    Run `docker ps -a` to see output like this:
    
            IMAGE                STATUS                        NAMES
        ... openaddr/machine ... Exited (0) 44 seconds ago ... openaddressesmachine_machine_1
            mdillon/postgis      Up 45 seconds                 openaddressesmachine_postgres_1

4.  Connect to the OpenAddresses image `openaddr/machine` with a bash shell
    and the current working directory mapped to `/vol`:
    
        docker-compose run machine bash
    
5.  Build the OpenAddresses packages using
    [virtualenv](https://packaging.python.org/installing/#creating-virtual-environments)
    and [pip](https://packaging.python.org/installing/#use-pip-for-installing).
    The `-e` flag to `pip install` insures that your local copy of OpenAddresses
    is used, so that you can test changes to the code made in your own editor:
    
        pip3 install virtualenv
        virtualenv -p python3 --system-site-packages venv
        source venv/bin/activate
        pip3 install -e file:///vol
    
You should now be able to make changes and test them.
If you exit the Docker container, changes made in step 5 above will be lost.
Use [Docker commit](https://docs.docker.com/engine/reference/commandline/commit/)
or similar if you need to save them.

Run unit tests:

    python3 /vol/test.py

Running A First Set
-------------------

Run a [batch set](processes.md#batch-set) of address data to populate machine
with sample data. These instruction show how to run a set of small-scale testing
data from [the repository `openaddresses/minimal-test-sources`](https://github.com/openaddresses/minimal-test-sources).
This process should take less than 10 minutes.

1.  After preparing a virtual machine and running tests, a new local
    `openaddr` Postgres database will exist with this connection string:

        postgres://openaddr:openaddr@localhost/openaddr

    Three other pieces of information are needed:

    - An empty [Amazon S3 bucket](http://docs.aws.amazon.com/AmazonS3/latest/gsg/CreatingABucket.html) in the US Standard region to store data.
    - Amazon Web Services credentials [stored where `boto` can find them](http://boto.cloudhackers.com/en/latest/boto_config_tut.html).
    - A [personal access token](https://help.github.com/articles/creating-an-access-token-for-command-line-use/) to access Github’s API.

2.  In a terminal window, [run `openaddr-enqueue-sources`](components.md#enqueue)
    with the information above and leave it open and running:

        openaddr-enqueue-sources --verbose \
            --owner openaddresses --repository minimal-test-sources \
            --database-url {Connection String} \
            --github-token {Github Token} \
            --bucket {Amazon S3 Bucket Name}

3.  In a second terminal window, [run a single worker](components.md#worker) to
    processed the queued sources one after another, then [run the dequeuer](components.md#dequeuer)
    to pass them back. Note both of these programs do not exit, they merely block waiting for work. You can manually abort them with `Ctrl-C` once the work is completed.

        openaddr-ci-worker --verbose \
            --database-url {Connection String} \
            --bucket {Amazon S3 Bucket Name}

        env DATABASE_URL={Connection String} \
            GITHUB_TOKEN={Github Token} \
            openaddr-ci-run-dequeue

4.  Back in the first terminal window, you should have seen `openaddr-enqueue-sources`
    complete and exit. You can now run the [Webhooks web application](components.md#webhook)
    and leave it running to see the results of the batch set in a web browser:

        env DATABASE_URL={Connection String} \
            GITHUB_TOKEN={Github Token} \
            AWS_S3_BUCKET={Amazon S3 Bucket Name} \
            python3 run-debug-webhooks.py

5.  In the second terminal window, try [collecting address data into downloadable archives](components.md#collect):

        openaddr-collect-extracts --verbose \
            --owner openaddresses --repository minimal-test-sources \
            --database-url {Connection String} \
            --bucket {Amazon S3 Bucket Name}
