Install
=======

This document describes how to install the Machine code for local development, and demonstrates two ways to use it: running a single source and running a complete batch set. If you’re editing a lot of sources and want to do it quickly without waiting for a remote Github-based continuous integration service, you may want to use run single sources locally. If you're working on the queuing and job control portions of Machine code, you may want to run complete batch sets on test data.

Local Development
-----------------

You can edit a local copy of OpenAddresses code with working tests by installing
everything onto a local virtual machine using [VirtualBox](https://www.virtualbox.org)
and [Vagrant](https://www.vagrantup.com). This process should take about 10-20
minutes depending on download speed.

1.  Download and install [VirtualBox](https://www.virtualbox.org) and [Vagrant](https://www.vagrantup.com) on your development machine. Both are available as separate installs, or as [part of Homebrew](https://brew.sh).
    
    Ensure that `VBoxManage` is in your path. If you download [VirtualBox from the website](https://www.virtualbox.org/wiki/Downloads), `VBoxManage` may be located in `/Applications/VirtualBox.app/Contents/MacOS` and you will need to [add it to your shell path](https://kb.iu.edu/d/acar).

2.  Clone [OpenAddresses Machine code](https://github.com/openaddresses/machine) from Github.

3.  From inside the machine folder, prepare the VirtualBox virtual machine with this command:

        vagrant up
    
    You’ll see a few notices scroll by to know that this process is working:
    
        ==> default: Importing base box 'ubuntu/trusty64'...
        ==> default: Setting the name of the VM: OpenAddresses-Machine_default_1487786156783_59682
        ==> default: Waiting for machine to boot. This may take a few minutes...
        ==> default: Machine booted and ready!
    
    This last part can take ~5 minutes:
    
        ==> default: Mounting shared folders...
            default: /vagrant => /Users/jrandom/Sites/OpenAddresses-Machine
        ==> default: Running provisioner: shell...
            default: Running: inline script

4.  Connect to the virtual machine with this command:
    
        vagrant ssh

5.  Run the complete test suite to verify that it works:
    
        python3 /vagrant/test.py

You should now be able to make changes and test them. The virtual machine’s
`/vagrant` directory is a mount of your host machine’s current directory, so you
will be able to edit files in your normal text editor. Be sure to use `pip3` and
`python3` when running, or [set up an optional quick local virtual environment](http://docs.python-guide.org/en/latest/dev/virtualenvs/)
with Python 3 and the [`--editable` flag](https://pip.pypa.io/en/stable/reference/pip_install/#install-editable).

Running A First Source
----------------------

You can process a single individual source of OpenAddresses data with the command `openaddr-process-one` and a source JSON file. This will let you verify tests and behavior locally.

1.  Download a source from [OpenAdresses/openaddresses on Github](https://github.com/openaddresses/openaddresses). [Berkeley, California](https://results.openaddresses.io/sources/us/ca/berkeley) is a small, reliable source that’s good to test with:
    
        curl -L https://github.com/openaddresses/openaddresses/raw/master/sources/us/ca/berkeley.json -o us-ca-berkeley.json

2.  Run `openaddr-process-one` to process the source:
    
        openaddr-process-one -v us-ca-berkeley.json .

3.  Look in the directory `us-ca-berkeley` for address output, logs, and other files.

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
