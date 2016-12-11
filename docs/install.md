Install
=======

Local Development
-----------------

You can edit a local copy of OpenAddresses code with working tests by installing
everything onto an Ubuntu Linux virtual machine. We recommend using a dedicated
virtual machine for development; our Chef script installs servers, packages, and
other configuration that may disrupt other uses of an Ubuntu machine. This
process should take about 20 minutes depending on download speed, or you can
skip the first four steps by using a pre-built Ubuntu 14.04 image on Amazon EC2.

1.  Download and install [VirtualBox](https://www.virtualbox.org/wiki/Downloads) on your development machine.
    
2.  Download an [Ubuntu 14.04 Trusty server install image](http://releases.ubuntu.com/14.04/). A good choice might be `ubuntu-14.04.4-server-amd64.iso`.
    
3.  Create a new virtual machine, and configure its NAT network adapter so you can SSH into the machine [as described in this guide](http://stackoverflow.com/questions/5906441/how-to-ssh-to-a-virtualbox-guest-externally-through-a-host#10532299). Note that you’ll be SSHing into `127.0.0.1`, not the VM’s address.
    
4.  Install Ubuntu 14.04 on the new machine, and log in.
    
5.  Clone [OpenAddresses Machine code](https://github.com/openaddresses/machine) from Github.
    
6.  From inside the new `machine` directory, install the code for local development. This might take a few minutes the first time. `chef/run.sh` is safe to run multiple times:
    
        sudo chef/run.sh localdev
    
7.  Run the complete test suite to verify that it works:
    
        python3 test.py

You should now be able to make changes and test them. Be sure to use `pip3` and
`python3` when running, or [set up an optional quick local virtual environment](http://docs.python-guide.org/en/latest/dev/virtualenvs/)
with Python 3.

Running A First Set
-------------------

Run a [batch set](processes.md#batch-set) of address data to populate machine
with sample data. These instruction show how to run a set of small-scale testing
data from [the repository `openaddresses/minimal-test-sources`](https://github.com/openaddresses/minimal-test-sources).
This process should take less than 10 minutes.

1.  After installing the `localdev` chef role and running tests, a new local
    `openaddr` Postgres database will exist with this connection string:
    
        postgres://openaddr:openaddr@localhost/openaddr
    
    Three other pieces of information are needed:
    
    - An empty [Amazon S3 bucket](http://docs.aws.amazon.com/AmazonS3/latest/gsg/CreatingABucket.html) to store data.
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
    to pass them back:
    
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

Production
----------

🚧 This information is currently out-of-date 🚧

You may need to install machine from scratch when deploying a new version in parallel with an old one.

1.  Create a Postgres 9 database instance (Machine is tested with and currently runs on 9.3). On [AWS RDS](https://aws.amazon.com/rds/), choose a `db.t2.micro` sized instance, since Machine only stores metadata about sources in its database. Note the access credentials to the database.
2.  Set up a new [AWS SNS](https://aws.amazon.com/sns/) topic for notifications. This will provide a channel for error messages and exceptions, if any occur. Note its [resource name (ARN)](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html).
3.  Set up a new [AWS S3](https://aws.amazon.com/s3/) bucket for output data.
4.  Prepare a new Ubuntu 14.04 instance. On [AWS EC2](https://aws.amazon.com/ec2/), use a stock _Quick Start_ machine image (AMI). Note its public URL.
5.  Register a new [Github OAuth application](https://developer.github.com/v3/oauth/). Use the public URL of the server instance to construct a callback URL like `http://{hostname}/auth/callback`. Note its unique client ID and client secret.
6.  Clone the [`openaddresses/machine` repository](https://github.com/openaddresses/machine) to the Ubuntu server. Configure the server hostname, database, AWS, Github, and other details from above in `chef/data/local.json`.
7.  Install machine software using the command `chef/run.sh openaddr`.
8.  Initialize database tables using the command `openaddr-ci-recreate-db`.
9.  Complete the machine using the commands `chef/run.sh webhooks` and `chef/run.sh worker` to install everything.
10. Verify that it works in a web browser.
10. Add the new environment webhook to a repository of OpenAddresses sources with a URL like `http://{hostname}/hook`.
