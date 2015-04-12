Here's step-by-step instructions for how to run these examples from scratch.
This should work the same on any OS.
Dependencies: Git and Vagrant.

### Step 1: Clone repository

    $ git clone https://github.com/tormaroe/beanstalkd.nim
    $ cd beanstalkd.nim

### Step 2: Set up environment

This will set up a Linux VM with the nim compiler and a running beanstalkd service:

    $ vagrant up

Then log in to the VM shell:

    $ vagrant ssh

### Step 3: Build arn run all examples

    $ cd /vagrant
    $ ./test

The example executables are created in /vagrant/build
