#!/bin/bash -e
#
# Install the chef ruby gem if chef-solo is not in the path.
# This script is safe to run multiple times.
#
if [ ! `which chef-solo` ]; then
    release=`lsb_release -r`
    if [ "$release" != "${release/12.04/}" ]; then
        # Ruby 1.9.3 install provided for Ubuntu 12.04
        apt-get install -y build-essential ruby1.9.3
        gem1.9.3 install chef -v 11.16.4 --no-rdoc --no-ri
    else
        # Otherwise, assume Ubuntu ~14+
        apt-get install -y build-essential ruby ruby-dev
        gem install chef -v 11.16.4 --no-rdoc --no-ri
    fi
fi

cd `dirname $0`
chef-solo -c $PWD/solo.rb -j $PWD/role-ubuntu.json
