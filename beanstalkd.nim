# Copyright (C) 2015 Torbjørn Marø
# MIT License

## This module provides a client API to a `beanstalkd
## <http://kr.github.io/beanstalkd/>`_ server for both producers
## and consumers.
##
## If you want more details on the underlying beanstalkd protocol see
## `this protocol specification
## <https://github.com/kr/beanstalkd/blob/master/doc/protocol.txt>`_.
##
## Job lifecycle
## =============
## A job in beanstalk gets created by a client with the "put" command. During its
## life it can be in one of four states: "ready", "reserved", "delayed", or
## "buried". After the put command, a job typically starts out ready. It waits in
## the ready queue until a worker comes along and runs the "reserve" command. If
## this job is next in the queue, it will be reserved for the worker. The worker
## will execute the job; when it is finished the worker will send a "delete"
## command to delete the job.

import net, strutils

type
  Job* = tuple ## \
    ## Represents a job retrieved from beanstalkd.
    id: int
    data: string

# TODO: Define and use errors (based on protocol)

# -----------------------------------------------------------------------------
#  Private utility stuff ..
# -----------------------------------------------------------------------------

proc recvLine(socket: Socket) : string =
  var data: TaintedString = ""
  while data == "" or data == "\r\n": # TODO: Hack or not? A bit dangerous.
    socket.readLine(data)
  result = data

proc recvData(socket: Socket; parts: seq[string]; index: int) : string =
  var data: TaintedString = ""
  discard socket.recv(data, parts[index].parseInt)
  result = data

# -----------------------------------------------------------------------------
#  .. end private utility stuff
# -----------------------------------------------------------------------------

proc open*(address: string; port = Port(11300)) : Socket =
  ## Opens a socket to a beanstalkd server.
  result = newSocket()
  result.connect(address, port)

proc use*(socket: Socket; tube: string) : bool =
  ## Used by job producers to specify which tube to put jobs to.
  ## By default jobs go to the ``default`` tube.
  socket.send("use " & tube & "\r\n")
  var parts = socket.recvLine.split
  result = (parts[0] == "USING")

proc listTubes*(socket: Socket) =
  # TODO: Parse YAML and return a seg[string]
  socket.send("list-tubes\r\n")
  var parts = socket.recvLine().split
  if parts[0] == "OK":
    var data = socket.recvData(parts, 1)
    echo data
  else:
    echo "UNABLE TO LIST TUBES"

proc putStr*(socket: Socket; data: string; pri = 100; delay = 0; ttr = 5) : int =
  let command = "put $# $# $# $#\r\n$#\r\n" % [$pri, $delay, $ttr, $(data.len), data]
  socket.send(command)
  let parts = socket.recvLine.split
  if parts[0] == "INSERTED":
    result = parts[1].parseInt
  else:
    result = -1

proc reserve*(socket: Socket; timeout = -1) : Job =
  ## Reserve and return a job. If no job is available to be reserved but
  ## a ``timeout > 0`` is specified, reserve will block and wait the specified
  ## amount of seconds or until a job becomes available.
  ## If no job can be reserved, a job with ``id = -1`` is returned.
  if timeout < 0:
    socket.send("reserve\r\n")
  else:
    socket.send("result-with-timeout $#\r\n" % $timeout)
  let parts = socket.recvLine.split
  if parts[0] == "RESERVED":
    var data = socket.recvData(parts, 2)
    result = (id: parts[1].parseInt, data: data)
  else:
    result = (id: -1, data: "")

proc release*(socket: Socket; id: int; pri = 100; delay = 0) : bool =
  socket.send("release $# $# $#\r\n" % [$id, $pri, $delay])
  let response = socket.recvLine
  if response == "RELEASED":
    result = true
  else:
    result = false

proc delete*(socket: Socket; id: int) : bool =
  socket.send("delete $#\r\n" % $id)
  result = (socket.recvLine == "DELETED")

# -----------------------------------------------------------------------------
#  Code below only included if beanstalkd.nim is compiled as an executable.
# -----------------------------------------------------------------------------

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


    echo s.release(job.id)

    echo s.delete(job.id)

    echo "done."

  test()
