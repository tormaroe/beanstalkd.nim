# -*- mode: ruby -*-
# vi: set ft=ruby :

# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
VAGRANTFILE_API_VERSION = "2"

$bootstrap = <<SCRIPT

apt-get update
apt-get install -y git unzip beanstalkd

wget https://github.com/schickling/beanstalkd-cli/releases/download/0.3.0/beanstalkd-cli-linux.tar.gz
tar xvf beanstalkd-cli-linux.tar.gz

wget http://nim-lang.org/download/nim-0.10.2.zip
unzip nim-0.10.2.zip
cd nim-0.10.2
sh build.sh
ln -s /home/vagrant/nim-0.10.2/bin/nim /usr/bin/nim
cd ~

git clone https://github.com/nim-lang/nimble.git
cd nimble
nim c -r src/nimble install
cd ..
ln -s /home/vagrant/.nimble/bin/nimble /usr/bin/nimble
cd ~

nimble update

SCRIPT

$statup = <<SCRIPT

SCRIPT

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|

  config.vm.box = "ubuntu/trusty64"

  if Vagrant.has_plugin?('vagrant-cachier')
    config.cache.scope = :box
  end

  config.vm.network "forwarded_port", guest: 11300, host: 11300

  config.vm.provider "virtualbox" do |vb|
    vb.name = 'beanstalkd.nim'
    vb.memory = 1024
    vb.cpus = 2
  end

  config.vm.provision "shell", inline: $bootstrap
  config.vm.provision "shell", inline: $statup, run: "always"
end
