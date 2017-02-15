# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure(2) do |config|
  config.vm.box     = "ubuntu/trusty64"

  config.vm.provision "shell", inline:
    "add-apt-repository -y ppa:openaddresses/ci
     apt-get update -y
     cd /vagrant;
     ./chef/run.sh localdev"

end
