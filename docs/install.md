Install
=======

Local Development
-----------------

You can edit a local copy of OpenAddresses code with working tests by installing everything onto a Virtualbox virtual Ubuntu Linux machine.

1.  Download and install [VirtualBox](https://www.virtualbox.org/wiki/Downloads) on your development machine.
2.  Download an [Ubuntu 14.04 Trusty server install image](http://releases.ubuntu.com/14.04/). A good choice might be `ubuntu-14.04.4-server-amd64.iso`.
3.  Create a new virtual machine, and configure its NAT network adapter so you can SSH into the machine [as described in this guide](http://stackoverflow.com/questions/5906441/how-to-ssh-to-a-virtualbox-guest-externally-through-a-host#10532299).
4.  Install Ubuntu 14.04 on the new machine, and log in.
5.  Clone [OpenAddresses Machine code](https://github.com/openaddresses/machine) from Github.
6.  From inside the new `machine` directory, install the code for local development:
    
        sudo chef/run.sh localdev
7.  Run the complete test suite to verify that it works:
    
        python3 test.py
8.  You should now be able to make changes and test them.

Production
----------

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
