# Copyright (c) 2015 Torbjørn Marø
# MIT License

import beanstalkd, strutils, threadpool

# Same example as produce_and_consume_concurrent.nim, except that all multiple
# consumers of tube A are created to filter numbers in parallel.
#
# Note: compile with --threads:on
#
# Output will be something like:
#
#     produceNumbers done
#     All threads spawned
#     A-consumer 2 done, consumed 332, produced 156
#     A-consumer 3 done, consumed 328, produced 148
#     A-consumer 4 done, consumed 0, produced 0
#     A-consumer 1 done, consumed 339, produced 162
#     The sum of all multiples of 3 and 5 is 233168

proc produceNumbers() =
  let client = beanstalkd.open("127.0.0.1")
  discard client.use "A"
  for n in 1.. < 1000:
    discard client.put($n)
  echo "produceNumbers done"

proc consumeA(id: int) =
  let client = beanstalkd.open("127.0.0.1")
  discard client.watch "A"
  discard client.ignore "default"
  discard client.use "B"
  var consumed, produced : int
  while true:
    let next = client.reserve(timeout = 1)
    if next.success:
      consumed += 1
      let n = next.job.parseInt
      if (n mod 3 == 0) or (n mod 5 == 0):
        produced += 1
        discard client.put(next.job)
      discard client.delete(next.id)
    else:
      break
  echo "A-consumer $# done, consumed $#, produced $#" %
    [$id, $consumed, $produced]

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
for i in 1 .. 4:
  spawn consumeA(i)
let sum = spawn consumeB()
echo "All threads spawned"
echo "The sum of all multiples of 3 and 5 is " & $(^sum)
