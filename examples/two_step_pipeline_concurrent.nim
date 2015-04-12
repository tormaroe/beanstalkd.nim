# Copyright (c) 2015 Torbjørn Marø
# MIT License

import beanstalkd, strutils, threadpool

# Same example as produce_and_consume.nim, except that all three steps
# of the process happens in their own threads.
#
# Note: compile with --threads:on
#
# Output will be:
#
#     All threads spawned
#     produceNumbers done
#     consumeA done
#     The sum of all multiples of 3 and 5 is 233168

proc produceNumbers() =
  let client = beanstalkd.open("127.0.0.1")
  discard client.use "A"
  for n in 1.. < 1000:
    discard client.put($n)
  echo "produceNumbers done"

proc consumeA() =
  let client = beanstalkd.open("127.0.0.1")
  discard client.watch "A"
  discard client.ignore "default"
  discard client.use "B"
  while true:
    let next = client.reserve(timeout = 1)
    if next.success:
      let n = next.job.parseInt
      if (n mod 3 == 0) or (n mod 5 == 0):
        discard client.put(next.job)
        discard client.delete(next.id)
    else:
      break
  echo "consumeA done"

proc consumeB() : int =
  let client = beanstalkd.open("127.0.0.1")
  discard client.watch "B"
  discard client.ignore "default"
  var sum = 0
  while true:
    let next = client.reserve(timeout = 1)
    if next.success:
      sum += next.job.parseInt
      discard client.delete(next.id)
    else:
      break
  result = sum

spawn produceNumbers()
spawn consumeA()
let sum = spawn consumeB()
echo "All threads spawned"
echo "The sum of all multiples of 3 and 5 is " & $(^sum)
