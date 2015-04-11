
![beanstalkd.nim logo](https://raw.githubusercontent.com/tormaroe/beanstalkd.nim/master/gfx/logo.png)

Status: Pre-Alpha / under development

## Installation

How to install with nimble ...

## Usage

How to connect, put, reserve, delete, etc. Refer to docs for details, and to beanstalkd documentation ...

```nim
import beanstalkd

let beanstalk = beanstalkd.open("127.0.0.1")
beanstalk.putStr("This is a job")
beanstalk.putStr("Top priority job", pri = 1)

let job = s.reserve
echo job
 #==> (success: true, status: reserved, jobId: 42, job: Top priority job)

let result = beanstalk.delete(job.jobId)
echo result
 #==> (success: true, status: deleted)
```

## Development

How to run tests, prepare a new release, generate docs, etc. ...
