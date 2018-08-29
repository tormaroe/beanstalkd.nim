# Copyright (c) 2015 Torbjørn Marø
# MIT License

# Example of how to use beanstalkd/httpreceiver to accept incoming http
# requests and put them on a queue.

import
  asyncdispatch,
  beanstalkd/httpreceiver

echo "Starting http receiver at port 8080"

addHandler("/", proc (body: string): HandlerResult =
  echo "* Invoking normal handler *"
  result.pri = 2000
  result.response = "job accepted")

addHandler("/high", proc (body: string): HandlerResult =
  echo "* Invoking high priority handler *"
  if len(body) > 10:
    result.reject = true
    result.response = "job too BIG for high priority!"
  else:
    result.pri = 10
    result.response = "high priority job accepted")

runReceiver(port = Port(8080))
