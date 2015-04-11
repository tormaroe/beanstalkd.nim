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
  StatusCode* = enum
    ok,
    outOfMemory,
    internalError,
    badFormat,
    unknownCommand,
    inserted,
    buried,
    expectedCrLf,
    jobTooBig,
    draining,
    usingOk,
    deadlineSoon,
    timedOut,
    paused,
    reserved,
    deleted,
    notFound,
    released,
    touched,
    kicked,
    found,
    watching,
    notIgnored,
    unknownResponse

type
  BeanResponse* = tuple
    success: bool
    status: StatusCode

type
  BeanIntResponse* = tuple
    success: bool
    status: StatusCode
    value: int

type
  BeanJob* = tuple
    success: bool
    status: StatusCode
    id: int
    job: string

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

proc getCommonStatusCode(resp: string) : StatusCode =
  result = case resp
    of "USING": StatusCode.usingOk
    of "OUT_OF_MEMORY": StatusCode.outOfMemory
    of "INTERNAL_ERROR": StatusCode.internalError
    of "BAD_FORMAT": StatusCode.badFormat
    of "UNKNOWN_COMMAND": StatusCode.unknownCommand
    of "NOT_FOUND": StatusCode.notFound
    else: StatusCode.unknownResponse

proc beanInt(code: StatusCode; value = 0; success = true) : BeanIntResponse =
  (success: success, status: code, value: value)

proc beanJob(code: StatusCode; success = true; id = 0; jobData = "") : BeanJob =
  (success: success, status: code, id: id, job: jobData)

proc yamlToSeq(yaml: string) : seq[string] =
  ## NOT a complete YAML parser at all, just method to parse the simple YAML
  ## lists returned by beanstalkd.
  result = @[]
  for line in yaml.splitLines:
    if line.startsWith "- ":
      result.add line[2 .. -1]

proc recvStats(socket: Socket) : seq[string] =
  var parts = socket.recvLine.split
  if parts[0] == "OK":
    result = socket.recvData(parts, 1).splitLines[1 .. -2]

proc recvFoundJob(socket: Socket) : BeanJob =
  let parts = socket.recvLine.split
  result = case parts[0]
    of "FOUND": StatusCode.found.beanJob(
      id = parts[1].parseInt,
      jobData = socket.recvData(parts, 2))
    else: getCommonStatusCode(parts[0]).beanJob(success= false)


# -----------------------------------------------------------------------------
#  .. end private utility stuff
# -----------------------------------------------------------------------------

proc open*(address: string; port = Port(11300)) : Socket =
  ## Opens a socket to a beanstalkd server.
  result = newSocket()
  result.connect(address, port)

proc use*(socket: Socket; tube: string) : BeanResponse =
  ## Used by job producers to specify which tube to put jobs to.
  ## By default jobs go to the ``default`` tube.
  socket.send("use " & tube & "\r\n")
  var parts = socket.recvLine.split
  result = case parts[0]
    of "USING": (success: true, status: StatusCode.usingOk)
    else: (success: false, status: getCommonStatusCode(parts[0]))

proc watch*(socket: Socket; tube: string) : BeanIntResponse =
  ## The "watch" command adds the named tube to the watch list for the current
  ## connection. A reserve command will take a job from any of the tubes in the
  ## watch list. For each new connection, the watch list initially consists of one
  ## tube, named "default".
  ##
  ## ``tube`` is a name at most 200 bytes. It specifies a tube to add to the watch
  ## list. If the tube doesn't exist, it will be created.
  ##
  ## ``watch`` returns the integer number of tubes currently in the watch list.
  socket.send("watch " & tube & "\r\n")
  let response = socket.recvLine.split
  result = case response[0]
    of "WATCHING": StatusCode.watching.beanInt(value= response[1].parseInt)
    else: getCommonStatusCode(response[0]).beanInt(success= false)

proc ignore*(socket: Socket; tube: string) : BeanIntResponse =
  socket.send("ignore " & tube & "\r\n")
  let response = socket.recvLine.split
  result = case response[0]
    of "WATCHING": StatusCode.watching.beanInt(value= response[1].parseInt)
    of "NOT_IGNORED": StatusCode.notIgnored.beanInt(success= false)
    else: getCommonStatusCode(response[0]).beanInt(success= false)

proc listTubes*(socket: Socket) : seq[string] =
  socket.send("list-tubes\r\n")
  var parts = socket.recvLine().split
  if parts[0] == "OK":
    result = socket.recvData(parts, 1).yamlToSeq

proc putStr*(socket: Socket; data: string; pri = 100; delay = 0; ttr = 5) : BeanIntResponse =
  let command = "put $# $# $# $#\r\n$#\r\n" % [$pri, $delay, $ttr, $(data.len), data]
  socket.send(command)
  let parts = socket.recvLine.split
  result = case parts[0]
    of "INSERTED": StatusCode.inserted.beanInt(value= parts[1].parseInt)
    of "BURIED": StatusCode.buried.beanInt(value= parts[1].parseInt)
    of "EXPECTED_CRLF": StatusCode.expectedCrLf.beanInt(success= false)
    of "JOB_TOO_BIG": StatusCode.jobTooBig.beanInt(success= false)
    of "DRAINING": StatusCode.draining.beanInt(success= false)
    else: getCommonStatusCode(parts[0]).beanInt(success= false)

proc reserve*(socket: Socket; timeout = -1) : BeanJob =
  ## Reserve and return a job. If no job is available to be reserved but
  ## a ``timeout > 0`` is specified, reserve will block and wait the specified
  ## amount of seconds or until a job becomes available.
  ## If no job can be reserved, a job with ``id = -1`` is returned.
  if timeout < 0:
    socket.send("reserve\r\n")
  else:
    socket.send("reserve-with-timeout $#\r\n" % $timeout)
  let parts = socket.recvLine.split
  result = case parts[0]
    of "RESERVED": StatusCode.reserved.beanJob(
      id = parts[1].parseInt,
      jobData = socket.recvData(parts, 2))
    of "DEADLINE_SOON": StatusCode.deadlineSoon.beanJob(success= false)
    of "TIMED_OUT": StatusCode.timedOut.beanJob(success= false)
    else: getCommonStatusCode(parts[0]).beanJob(success= false)

proc peek*(socket: Socket; id: int) : BeanJob =
  socket.send("peek $#\r\n" % $id)
  result = socket.recvFoundJob

proc peekReady*(socket: Socket) : BeanJob =
  socket.send("peek-ready\r\n")
  result = socket.recvFoundJob

proc peekDelayed*(socket: Socket) : BeanJob =
  ## Returns the delayed job with the shortest delay left.
  socket.send("peek-delayed\r\n")
  result = socket.recvFoundJob

proc peekBuried*(socket: Socket) : BeanJob =
  ## Returns the next job in the list of buried jobs.
  socket.send("peek-buried\r\n")
  result = socket.recvFoundJob

proc release*(socket: Socket; id: int; pri = 100; delay = 0) : BeanResponse =
  socket.send("release $# $# $#\r\n" % [$id, $pri, $delay])
  let response = socket.recvLine
  result = case response
    of "RELEASED": (success: true, status: StatusCode.released)
    of "BURIED": (success: false, status: StatusCode.buried)
    else: (success: false, status: response.getCommonStatusCode)

proc touch*(socket: Socket; id: int) : BeanResponse =
  socket.send("touch $#\r\n" % $id)
  let response = socket.recvLine
  result = case response
    of "TOUCHED": (success: true, status: StatusCode.touched)
    else: (success: false, status: response.getCommonStatusCode)

proc delete*(socket: Socket; id: int) : BeanResponse =
  socket.send("delete $#\r\n" % $id)
  let response = socket.recvLine
  result = case response
    of "DELETED": (success: true, status: StatusCode.deleted)
    else: (success: false, status: response.getCommonStatusCode)

proc bury*(socket: Socket; id: int, pri = 100) : BeanResponse =
  socket.send("bury $# $#\r\n" % [$id, $pri])
  let response = socket.recvLine
  result = case response
    of "BURIED": (success: true, status: StatusCode.buried)
    else: (success: false, status: response.getCommonStatusCode)

proc kickJob*(socket: Socket; id: int) : BeanResponse =
  socket.send("kick-job $#\r\n" % $id)
  let response = socket.recvLine
  result = case response
    of "KICKED": (success: true, status: StatusCode.kicked)
    else: (success: false, status: response.getCommonStatusCode)

proc kick*(socket: Socket; bound: int) : BeanIntResponse =
  ## The kick command applies only to the currently used tube. It moves jobs into
  ## the ready queue. If there are any buried jobs, it will only kick buried jobs.
  ## Otherwise it will kick delayed jobs.
  ##
  ## ``bound`` is an integer upper bound on the number of jobs to kick. The server
  ## will kick no more than ``bound`` jobs.
  ##
  ## ``kick`` returns an integer indicating the number of jobs actually kicked.
  socket.send("kick $#\r\n" % $bound)
  let parts = socket.recvLine.split
  result = case parts[0]
    of "KICKED": StatusCode.kicked.beanInt(value= parts[1].parseInt)
    else: getCommonStatusCode(parts[0]).beanInt(success= false)

proc stats*(socket: Socket) : seq[string] =
  socket.send("stats\r\n")
  result = socket.recvStats

proc statsTube*(socket: Socket, tube: string) : seq[string] =
  socket.send("stats-tube $#\r\n" % tube)
  result = socket.recvStats

proc statsJob*(socket: Socket, id: int) : seq[string] =
  socket.send("stats-job $#\r\n" % $id)
  result = socket.recvStats

proc listTubeUsed*(socket: Socket) : string =
  socket.send("list-tube-used\r\n")
  var parts = socket.recvLine.split
  if parts[0] == "USING":
    result = parts[1]

proc listTubesWatched*(socket: Socket) : seq[string] =
  socket.send("list-tubes-watched\r\n")
  var parts = socket.recvLine.split
  if parts[0] == "OK":
    result = socket.recvData(parts, 1).yamlToSeq

proc pauseTube*(socket: Socket; tube: string; delay: int) : BeanResponse =
  socket.send("pause-tube $# $#\r\n" % [tube, $delay])
  let response = socket.recvLine
  result = case response
    of "PAUSED": (success: true, status: StatusCode.paused)
    else: (success: false, status: getCommonStatusCode(response))

proc quit*(socket: Socket) =
  socket.send("quit\r\n")

# -----------------------------------------------------------------------------
#  Code below only included if beanstalkd.nim is compiled as an executable.
# -----------------------------------------------------------------------------

when isMainModule:
  proc test() =
    assert 1 == 1

    let s = open("127.0.0.1")
    #s.use("foobar")
    let tubes = s.listTubes()

    echo s.listTubeUsed
    echo s.listTubesWatched

    for t in tubes:
      echo "TUBE $#" % t

    echo s.putStr("foo")

    let job = s.reserve
    echo job

    echo "PEEK-READY:"
    echo s.peekReady
    echo "PEEK-BURIED:"
    echo s.peekBuried

    echo s.statsJob(job.id)
    echo s.release(job.id)
    echo s.peek(job.id)
    echo s.peek(122)
    echo s.delete(job.id)

    echo "PEEK-READY:"
    echo s.peekReady

    echo s.stats
    echo s.statsTube "default"

    echo "PEEK-DELAYED:"
    echo s.peekDelayed

    echo s.putStr("a delayed job", delay = 10)

    echo "PEEK-DELAYED:"
    echo s.peekDelayed

    echo s.pauseTube("default", 10)

    s.quit

    echo "done."

  test()
