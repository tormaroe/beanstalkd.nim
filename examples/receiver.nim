# Copyright (c) 2015 Torbjørn Marø
# MIT License

# Example of how to use beanstalkd/httpreceiver to accept incoming http
# requests and put them on a queue.

import
  asyncdispatch,
  beanstalkd/httpreceiver

echo "Starting http receiver at port 8080"

addHandler("/", proc (body: string; res: var HandlerResult): string =
  echo "HANDLED"
  res.response = "fooooooooooooooooooooooooooo"
  return body)

addHandler("/high", proc (body: string; res: var HandlerResult): string =
  echo "HANDLED HIGH"
  res.response = "baaaaaaaaaaaaaaaaaaaaaaar"
  return body)

runReceiver(port = Port(8080))
