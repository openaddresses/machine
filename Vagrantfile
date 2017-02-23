# -*- mode: ruby -*-
# vi: set ft=ruby :
# 
# Read more about how this file is used in docs/install.md.

Vagrant.configure(2) do |config|
  config.vm.box = "ubuntu/trusty64"

  config.vm.provision "shell", inline:
    "/vagrant/chef/run.sh localdev"

end
