#!/bin/sh -e
#
# Install the chef ruby gem if chef-solo is not in the path.
# This script is safe to run multiple times.
#
if [ ! `which chef-solo` ]; then
    apt-get install -y build-essential ruby ruby-dev
    gem install chef --no-rdoc --no-ri
fi

cd `dirname $0`
chef-solo -c $PWD/solo.rb -j $PWD/role-ubuntu.json
