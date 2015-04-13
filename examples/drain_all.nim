# Copyright (c) 2015 Torbjørn Marø
# MIT License

# Example output when run with two jobs in the default tube:
#
# *** DRAINING ALL TUBES ***
# +++ Watching tube `default` +++
# (success: true, status: watching, value: 1)
# (success: true, status: reserved, id: 95, job: fooo bar)
# (success: true, status: deleted)
# (success: true, status: reserved, id: 96, job: fooo bar 2)
# (success: true, status: deleted)
# (success: false, status: timedOut, id: 0, job: )
# *** DONE ***

import beanstalkd, strutils

echo "*** DRAINING ALL TUBES ***"

let client = beanstalkd.open("127.0.0.1")

for tube in client.listTubes:
  echo "+++ Watching tube `$#` +++" % tube
  echo client.watch(tube)

var count : int

while true:
  var job = client.reserve(timeout = 0)
  discard job
  if job.success:
    discard client.delete(job.id)
    count += 1
  else:
    echo "Drained $# jobs" % $count
    break

echo "*** DONE ***"
