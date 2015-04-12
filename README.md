
![beanstalkd.nim logo](https://raw.githubusercontent.com/tormaroe/beanstalkd.nim/master/gfx/logo.png)

This is a Nim client library to connect to and use [beanstalkd](http://kr.github.io/beanstalkd/).

Status: Beta, ready for experimental usage, may not be production ready.

## Installation

How to install with nimble ...

## Usage

How to connect, put, reserve, delete, etc. Refer to docs and examples for details, and to beanstalkd documentation ...

```nim
import beanstalkd

let beanstalk = beanstalkd.open("127.0.0.1")
beanstalk.putStr("This is a job")
beanstalk.putStr("Top priority job", pri = 1)

let job = s.reserve
echo job
 #==> (success: true, status: reserved, id: 42, job: Top priority job)

let result = beanstalk.delete(job.id)
echo result
 #==> (success: true, status: deleted)
```

## Development

This project uses vagrant to bootstrap the development environment. To get started:

    $ git clone https://github.com/tormaroe/beanstalkd.nim
    $ cd beanstalkd.nim
    $ vagrant up
    $ vagrant ssh
    $ cd /vagrant

Now to build and run all the examples, do:

    $ ./test

Executables are located in `/vagrant/build`. To only compile the beanstalkd module, do:

    $ ./compile

To re-generate module documentation, run:

    $ ./gendoc

TODO: How to prepare new release...
