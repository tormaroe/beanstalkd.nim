# Copyright (C) 2015 Torbjørn Marø
# MIT License

import net, strutils

type
  Job = tuple
    id: int
    data: string

const
  ok = "OK"

proc open(address: string; port = Port(11300)) : Socket =
  result = newSocket()
  result.connect(address, port)

proc use(socket: Socket; tube: string) =
  socket.send("use " & tube & "\r\n")
  var data: TaintedString = ""
  socket.readLine(data)
  var parts = data.split
  if parts[0] == "USING":
    echo "Now using " & parts[1]
  else:
    echo "Using did not work!"

proc listTubes(socket: Socket) =
  socket.send("list-tubes\r\n")
  var data: TaintedString = ""
  socket.readLine(data)
  var parts = data.split
  if parts[0] == ok:
    discard socket.recv(data, parts[1].parseInt)
    echo data
  else:
    echo "NOT OK!!"

proc putStr(socket: Socket; data: string; pri = 100; delay = 0; ttr = 5) : int =
  let command = "put $# $# $# $#\r\n$#\r\n" % [$pri, $delay, $ttr, $(data.len), data]
  socket.send(command)
  var data: TaintedString = ""
  socket.readLine(data) # not sure why I need this ?!?!
  socket.readLine(data)
  let parts = data.split
  if parts[0] == "INSERTED":
    result = parts[1].parseInt
  else:
    result = 0

proc reserve(socket: Socket; timeout = -1) : Job =
    if timeout < 0:
      socket.send("reserve\r\n")
    else:
      socket.send("result-with-timeout $#\r\n" % $timeout)
    var data: TaintedString = ""
    socket.readLine(data)
    let parts = data.split
    if parts[0] == "RESERVED":
      echo "Reserved job #" & parts[1]
      discard socket.recv(data, parts[2].parseInt)
      result = (id: parts[1].parseInt, data: data)
    else:
      echo "NO Reserve today"
      result = (id: -1, data: "")

proc delete(socket: Socket; id: int) : bool =
  socket.send("delete $#\r\n" % $id)
  var data: TaintedString = ""
  socket.readLine(data) # not sure why I need this ?!?!
  socket.readLine(data)
  if data == "DELETED":
    result = true
  else:
    result = false


when isMainModule:
  proc test() =
    assert 1 == 1

    var s = open("127.0.0.1")
    #s.use("foobar")
    s.listTubes()

    echo s.putStr("foo")

    let job = s.reserve
    job.id.`$`.echo
    echo job.data

    echo s.delete(job.id)

    echo "done."

  test()
