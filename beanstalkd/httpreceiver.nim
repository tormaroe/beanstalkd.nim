# Copyright (c) 2015 Torbjørn Marø
# MIT License

import
  beanstalkd,
  asynchttpserver,
  asyncdispatch,
  strtabs,
  tables

type
  HandlerResult* = object
    response* : string
  Handler* = proc (body:string; res: var HandlerResult): string

var
  handlers = initTable[string, Handler]()

proc handleRequest(req: Request) {.async.} =
  if handlers.hasKey(req.url.path):
    let h = handlers[req.url.path]
    var res = HandlerResult()
    discard h(req.body, res)
    await req.respond(Http200, res.response)
  else:
    await req.respond(Http404, "Not Found")

proc addHandler*(path : string; handler: Handler) =
  handlers[path] = handler

proc runReceiver*(address = ""; port = Port(80)) =
  var server = newAsyncHttpServer()
  asyncCheck server.serve(port, handleRequest, address)
  runForever()
