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
    reserved,
    deleted,
    notFound,
    released,
    touched,
    kicked,
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
    jobId: int
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
    else: StatusCode.unknownResponse

proc beanInt(code: StatusCode; value = 0; success = true) : BeanIntResponse =
  (success: success, status: code, value: value)

proc beanJob(code: StatusCode; success = true; jobId = 0; jobData = "") : BeanJob =
  (success: success, status: code, jobId: jobId, job: jobData)

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

proc listTubes*(socket: Socket) =
  # TODO: Parse YAML and return a seg[string]
  socket.send("list-tubes\r\n")
  var parts = socket.recvLine().split
  if parts[0] == "OK":
    var data = socket.recvData(parts, 1)
    echo data
  else:
    echo "UNABLE TO LIST TUBES"

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
    socket.send("result-with-timeout $#\r\n" % $timeout)
  let parts = socket.recvLine.split
  result = case parts[0]
    of "RESERVED": StatusCode.reserved.beanJob(
      jobId = parts[1].parseInt,
      jobData = socket.recvData(parts, 2))
    of "DEADLINE_SOON": StatusCode.deadlineSoon.beanJob(success= false)
    of "TIMED_OUT": StatusCode.timedOut.beanJob(success= false)
    else: getCommonStatusCode(parts[0]).beanJob(success= false)

proc release*(socket: Socket; id: int; pri = 100; delay = 0) : BeanResponse =
  socket.send("release $# $# $#\r\n" % [$id, $pri, $delay])
  let response = socket.recvLine
  result = case response
    of "RELEASED": (success: true, status: StatusCode.released)
    of "BURIED": (success: false, status: StatusCode.buried)
    of "NOT_FOUND": (success: false, status: StatusCode.notFound)
    else: (success: false, status: response.getCommonStatusCode)

proc touch*(socket: Socket; id: int) : BeanResponse =
  socket.send("touch $#\r\n" % $id)
  let response = socket.recvLine
  result = case response
    of "TOUCHED": (success: true, status: StatusCode.touched)
    of "NOT_FOUND": (success: false, status: StatusCode.notFound)
    else: (success: false, status: response.getCommonStatusCode)

proc delete*(socket: Socket; id: int) : BeanResponse =
  socket.send("delete $#\r\n" % $id)
  let response = socket.recvLine
  result = case response
    of "DELETED": (success: true, status: StatusCode.deleted)
    of "NOT_FOUND": (success: false, status: StatusCode.notFound)
    else: (success: false, status: response.getCommonStatusCode)

proc bury*(socket: Socket; id: int, pri = 100) : BeanResponse =
  socket.send("bury $# $#\r\n" % [$id, $pri])
  let response = socket.recvLine
  result = case response
    of "BURIED": (success: true, status: StatusCode.buried)
    of "NOT_FOUND": (success: false, status: StatusCode.notFound) # TODO NOT_FOUND can be moved into getCommonStatusCode
    else: (success: false, status: response.getCommonStatusCode)

proc kickJob*(socket: Socket; id: int) : BeanResponse =
  socket.send("kick-job $#\r\n" % $id)
  let response = socket.recvLine
  result = case response
    of "KICKED": (success: true, status: StatusCode.kicked)
    of "NOT_FOUND": (success: false, status: StatusCode.notFound)
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

# TODO "peek <id>\r\n" - return job <id>.
# TODO "peek-ready\r\n" - return the next ready job.
# TODO "peek-delayed\r\n" - return the delayed job with the shortest delay left.
# TODO "peek-buried\r\n" - return the next job in the list of buried jobs.
# TODO "stats-job <id>\r\n"
# TODO "stats-tube <tube>\r\n"
# TODO "stats\r\n"
# TODO "list-tube-used\r\n"
# TODO "list-tubes-watched\r\n"
# TODO "quit\r\n"
# TODO "pause-tube <tube-name> <delay>\r\n"

# -----------------------------------------------------------------------------
#  Code below only included if beanstalkd.nim is compiled as an executable.
# -----------------------------------------------------------------------------

when isMainModule:
  proc test() =
    assert 1 == 1

    let s = open("127.0.0.1")
    #s.use("foobar")
    s.listTubes()

    echo s.putStr("foo")

    let job = s.reserve
    echo job


    echo s.release(job.jobId)

    echo s.delete(job.jobId)

    echo "done."

  test()
