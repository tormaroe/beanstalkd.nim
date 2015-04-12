# Copyright (c) 2015 Torbjørn Marø
# MIT License

import beanstalkd, strutils

# A version of produce_and_consume.nim with an extra processing step.
# This example first puts all natural numbers below 1000 into tube 'A'.
# Then we retrieve them all again, filter out mutliples of 3 or 5, and inserts
# them into tube 'B'. Finally we retrieve all numbers in 'B' and sum it up.

let client = beanstalkd.open("127.0.0.1")

# Produce jobs in tube A..
echo client.use "A"
for n in 1.. < 1000:
  discard client.put($n)

# Consume jobs from tube A, produce jobs in tube B..
echo client.watch "A"
echo client.ignore "default"
echo client.use "B"
var count = 0
while true:
  let next = client.reserve(timeout = 0)
  if next.success:
    let n = next.job.parseInt
    if (n mod 3 == 0) or (n mod 5 == 0):
      discard client.put(next.job)
      discard client.delete(next.id)
      count += 1
  else:
    echo "Step A complete. $# jobs inserted into tube 'B'" % $count
    break

# Consume jobs from tube B, calculate sum
echo client.watch "B"
echo client.ignore "A"
var sum = 0
while true:
  let next = client.reserve(timeout = 0)
  if next.success:
    sum += next.job.parseInt
    discard client.delete(next.id)
  else:
    break

echo "The sum of all multiples of 3 and 5 is " & $sum
