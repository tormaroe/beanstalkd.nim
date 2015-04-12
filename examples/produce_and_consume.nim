# Copyright (c) 2015 Torbjørn Marø
# MIT License

import beanstalkd, strutils

# This example first puts a bunch of numbers in the default tube.
# Then we retrieve them all again and aggregate a total sum.
# It's actually demonstrates a solution to Project Euler problem
# number 1: Find the sum of all natural numbers below 1000 that is
# a multiple of 3 or 5.

let client = beanstalkd.open("127.0.0.1")

# Produce jobs..
for n in 1.. < 1000:
  if (n mod 3 == 0) or (n mod 5 == 0):
    discard client.put($n)

# Consume jobs..
var sum = 0
while true:
  let next = client.reserve(timeout = 0)
  if next.success:
    sum += next.job.parseInt
    discard client.delete(next.id)
  else:
    break

echo "The sum of all multiples of 3 and 5 is " & $sum
