# Copyright (c) 2015 Torbjørn Marø
# MIT License

import
  beanstalkd,
  asynchttpserver,
  asyncdispatch,
  strtabs,
  tables,
  net

type
  HandlerResult* = object
    response* : string
    pri* : int
    delay* : int
    ttr* : int
    job* : string
    reject* : bool
    tube* : string
  Handler* = proc (body:string): HandlerResult

var
  handlers = initTable[string, Handler]()

proc put(res: HandlerResult) =
  let
    tube = beanstalkd.open("127.0.0.1")
    ttr = if res.ttr == 0: 5 else: res.ttr
  discard tube.put(res.job, res.pri, res.delay, ttr)
  tube.quit

proc handleRequest(req: Request) {.async.} =
  if handlers.hasKey(req.url.path):
    let
      h = handlers[req.url.path]
      res = h(req.body)
    if res.reject:
      await req.respond(Http406, res.response)
    else:
      put(res) # TODO: Handle failure
      await req.respond(Http202, res.response)
  else:
    await req.respond(Http404, "Not Found")

proc addHandler*(path : string; handler: Handler) =
  handlers[path] = handler

proc runReceiver*(address = ""; port = Port(80)) =
  var server = newAsyncHttpServer()
  asyncCheck server.serve(port, handleRequest, address)
  runForever()
