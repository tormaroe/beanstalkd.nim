
![beanstalkd.nim logo](https://raw.githubusercontent.com/tormaroe/beanstalkd.nim/master/gfx/logo.png)

This is a Nim client library to connect to and use [beanstalkd](http://kr.github.io/beanstalkd/).

Status: Beta, ready for experimental usage, may not be production ready.

## Installation

[![Join the chat at https://gitter.im/tormaroe/beanstalkd.nim](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/tormaroe/beanstalkd.nim?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Install this library using the [nimble](https://github.com/nim-lang/nimble) package manager:

    nimble install beanstalkd

## Usage

*beanstalkd.nim* maps pretty closely to [the beanstalkd protocol](https://github.com/kr/beanstalkd/blob/master/doc/protocol.txt). For complete module documentation, see [tormaroe.github.io/beanstalkd.nim](http://tormaroe.github.io/beanstalkd.nim).

```nim
import beanstalkd

let client = beanstalkd.open("127.0.0.1")
discard client.put("hello world")

let job = client.reserve
echo job
 #==> (success: true, status: reserved, id: 42, job: hello world)

let result = client.delete(job.id)
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
