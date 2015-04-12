# Copyright (C) 2015 Torbjørn Marø
# MIT License

## This module provides a client API to a `beanstalkd
## <http://kr.github.io/beanstalkd/>`_ server for both producers
## and consumers.
##
## If you want more details on the underlying beanstalkd protocol, or the
## lifecycle of a beanstalk job, see
## `this protocol specification
## <https://github.com/kr/beanstalkd/blob/master/doc/protocol.txt>`_.

import net, strutils

type
  StatusCode* = enum ## \
    ## Represents all possible respons messages possible from the beanstalkd
    ## service.
    badFormat,
    buried,
    deadlineSoon,
    deleted,
    draining,
    expectedCrLf,
    found,
    inserted,
    internalError,
    jobTooBig,
    kicked,
    notFound,
    notIgnored,
    ok,
    outOfMemory,
    paused,
    released,
    reserved,
    timedOut,
    touched,
    unknownCommand,
    unknownResponse,
    usingOk,
    watching

type
  BeanResponse* = tuple ## \
    ## This type is returned by many of the beanstalkd procedures.
    ## It tells you if the operation was a success, and provides the status
    ## returned by the operation.
    success: bool
    status: StatusCode

type
  BeanIntResponse* = tuple ## \
    ## This type is returned by beanstalkd procedures which needs to return
    ## an integer.
    success: bool
    status: StatusCode
    value: int

type
  BeanJob* = tuple ## \
    ## This tuple is returned by beanstalkd procedures used to retrieve jobs.
    ## The actual job data is held by the ``job`` property.
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
  ## Opens a socket conection to a beanstalkd server.
  result = newSocket()
  result.connect(address, port)

proc listTubes*(socket: Socket) : seq[string] =
  ## Get a list of all available tubes.
  socket.send("list-tubes\r\n")
  var parts = socket.recvLine().split
  if parts[0] == "OK":
    result = socket.recvData(parts, 1).yamlToSeq

proc use*(socket: Socket; tube: string) : BeanResponse =
  ## Used by job producers to specify which tube to put jobs to.
  ## By default jobs go to the ``default`` tube (duh!).
  socket.send("use " & tube & "\r\n")
  var parts = socket.recvLine.split
  result = case parts[0]
    of "USING": (success: true, status: StatusCode.usingOk)
    else: (success: false, status: getCommonStatusCode(parts[0]))

proc listTubeUsed*(socket: Socket) : string =
  ## Get the name of the currently used tube
  ## (where new jobs will be put).
  ##
  ## The name is a bit strange, since it does not provide a list,
  ## but it's the name chosen by the beanstalkd author :)
  socket.send("list-tube-used\r\n")
  var parts = socket.recvLine.split
  if parts[0] == "USING":
    result = parts[1]

proc usedTube*(socket: Socket) : string =
  ## This is an alias for ``listTubeUsed``.
  result = socket.listTubeUsed

proc put*(socket: Socket; data: string; pri = 100; delay = 0; ttr = 5) : BeanIntResponse =
  ## Inserts a job into the currently used tube (see ``use``).
  ##
  ## ``data`` is the job body.
  ##
  ## ``pri`` is an integer < 2**32. Jobs with smaller priority values will be
  ## scheduled before jobs with larger priorities. The most urgent priority is 0;
  ## the least urgent priority is 4,294,967,295.
  ##
  ## ``delay`` is an integer number of seconds to wait before putting the job in
  ## the ready queue. The job will be in the "delayed" state during this time.
  ##
  ## ``ttr`` -- *time to run* -- is an integer number of seconds to allow a worker
  ## to run this job. This time is counted from the moment a worker reserves
  ## this job. If the worker does not delete, release, or bury the job within
  ## ttr seconds, the job will time out and the server will release the job.
  ## The minimum ttr is 1. If the client sends 0, the server will silently
  ## increase the ttr to 1.
  ##
  ## Upon success ``put`` returns a ``BeanIntResponse`` containing the id of the
  ## inserted job.
  ##
  ## Note that the server may run out of memory trying to grow the
  ## priority queue data structure. In that case the job should still be created,
  ## and a success with the new id is returned. The status will be ``buried``,
  ## not ``inserted`` as normal.
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

proc watch*(socket: Socket; tube: string) : BeanIntResponse =
  ## The ``watch`` command adds the named tube to the watch list for the current
  ## connection. A reserve command will take a job from any of the tubes in the
  ## watch list. For each new connection, the watch list initially consists of one
  ## tube, named "default".
  ##
  ## ``tube`` is a name at most 200 bytes. It specifies a tube to add to the watch
  ## list. If the tube doesn't exist, it will be created.
  ##
  ## ``watch`` returns the number of tubes currently in the watch list.
  socket.send("watch " & tube & "\r\n")
  let response = socket.recvLine.split
  result = case response[0]
    of "WATCHING": StatusCode.watching.beanInt(value= response[1].parseInt)
    else: getCommonStatusCode(response[0]).beanInt(success= false)

proc ignore*(socket: Socket; tube: string) : BeanIntResponse =
  ## Removes the named tube from the watch list for the current connection.
  socket.send("ignore " & tube & "\r\n")
  let response = socket.recvLine.split
  result = case response[0]
    of "WATCHING": StatusCode.watching.beanInt(value= response[1].parseInt)
    of "NOT_IGNORED": StatusCode.notIgnored.beanInt(success= false)
    else: getCommonStatusCode(response[0]).beanInt(success= false)

proc listTubesWatched*(socket: Socket) : seq[string] =
  ## Returns a sequence of named tubes that is currently watched by the
  ## connection. A reserve command will take a job from any of these tubes.
  socket.send("list-tubes-watched\r\n")
  var parts = socket.recvLine.split
  if parts[0] == "OK":
    result = socket.recvData(parts, 1).yamlToSeq

proc reserve*(socket: Socket; timeout = -1) : BeanJob =
  ## Reserve and return a job. If no job is available to be reserved but
  ## a ``timeout > 0`` is specified, reserve will block and wait the specified
  ## amount of seconds or until a job becomes available.
  ##
  ## The default timeout value of -1 makes ``reserve`` block and wait for new
  ## jobs indefinitely.
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

proc touch*(socket: Socket; id: int) : BeanResponse =
  ## ``touch`` allows a worker to request more time to work on a job.
  ## This is useful for jobs that potentially take a long time, but you still want
  ## the benefits of a TTR pulling a job away from an unresponsive worker.  A worker
  ## may periodically tell the server that it's still alive and processing a job.
  ## The command postpones the auto
  ## release of a reserved job until TTR seconds from when the command is issued.
  ##
  ## ``id`` is the ID of a job reserved by the current connection.
  socket.send("touch $#\r\n" % $id)
  let response = socket.recvLine
  result = case response
    of "TOUCHED": (success: true, status: StatusCode.touched)
    else: (success: false, status: response.getCommonStatusCode)

proc delete*(socket: Socket; id: int) : BeanResponse =
  ## Removes a job from the server entirely. It is normally used
  ## by the client when the job has successfully run to completion. A client can
  ## delete jobs that it has reserved, ready jobs, delayed jobs, and jobs that are
  ## buried.
  ##
  ## ``id`` is the job id to delete.
  socket.send("delete $#\r\n" % $id)
  let response = socket.recvLine
  result = case response
    of "DELETED": (success: true, status: StatusCode.deleted)
    else: (success: false, status: response.getCommonStatusCode)

proc release*(socket: Socket; id: int; pri = 100; delay = 0) : BeanResponse =
  ## Puts a reserved job back into the ready queue (and marks
  ## its state as "ready") to be run by any client.
  ##
  ## ``id``: The id of the job to release.
  ##
  ## ``pri``: A new priority to assign to the job.
  ##
  ## ``delay``: An integer number of seconds to wait before putting the job in
  ## the ready queue. The job will be in the "delayed" state during this time.
  socket.send("release $# $# $#\r\n" % [$id, $pri, $delay])
  let response = socket.recvLine
  result = case response
    of "RELEASED": (success: true, status: StatusCode.released)
    of "BURIED": (success: false, status: StatusCode.buried)
    else: (success: false, status: response.getCommonStatusCode)

proc bury*(socket: Socket; id: int, pri = 100) : BeanResponse =
  ## Puts a job into the "buried" state. Buried jobs are put into a
  ## FIFO linked list and will not be touched by the server again until a client
  ## kicks them with the ``kick`` command.
  ##
  ## ``id`` is the job id to bury.
  ##
  ## ``pri`` is a new priority to assign to the job.
  socket.send("bury $# $#\r\n" % [$id, $pri])
  let response = socket.recvLine
  result = case response
    of "BURIED": (success: true, status: StatusCode.buried)
    else: (success: false, status: response.getCommonStatusCode)

proc kick*(socket: Socket; bound: int) : BeanIntResponse =
  ## The kick command applies only to the currently used tube. It moves jobs into
  ## the ready queue. If there are any buried jobs, it will only kick buried jobs.
  ## Otherwise it will kick delayed jobs.
  ##
  ## ``bound`` is an integer upper bound on the number of jobs to kick. The server
  ## will kick no more than ``bound`` jobs.
  ##
  ## The returned respons contain an integer indicating the number of jobs
  ## actually kicked.
  socket.send("kick $#\r\n" % $bound)
  let parts = socket.recvLine.split
  result = case parts[0]
    of "KICKED": StatusCode.kicked.beanInt(value= parts[1].parseInt)
    else: getCommonStatusCode(parts[0]).beanInt(success= false)

proc kickJob*(socket: Socket; id: int) : BeanResponse =
  ## A variant of ``kick`` that operates with a single job
  ## identified by its job id. If the given job id exists and is in a buried or
  ## delayed state, it will be moved to the ready queue of the the same tube where it
  ## currently belongs.
  socket.send("kick-job $#\r\n" % $id)
  let response = socket.recvLine
  result = case response
    of "KICKED": (success: true, status: StatusCode.kicked)
    else: (success: false, status: response.getCommonStatusCode)

proc pauseTube*(socket: Socket; tube: string; delay: int) : BeanResponse =
  ## This command can delay any new job being reserved for a given time.
  ##
  ## ``tube`` is the tube to pause.
  ##
  ## ``delay`` is an integer number of seconds to wait before reserving any more
  ## jobs from the queue.
  socket.send("pause-tube $# $#\r\n" % [tube, $delay])
  let response = socket.recvLine
  result = case response
    of "PAUSED": (success: true, status: StatusCode.paused)
    else: (success: false, status: getCommonStatusCode(response))

proc peek*(socket: Socket; id: int) : BeanJob =
  ## Get a job by its id. The job is not reserved by this operation!
  socket.send("peek $#\r\n" % $id)
  result = socket.recvFoundJob

proc peekReady*(socket: Socket) : BeanJob =
  ## Get the highest priority job from the ready queue.
  ## The job is not reserved by this operation!
  socket.send("peek-ready\r\n")
  result = socket.recvFoundJob

proc peekDelayed*(socket: Socket) : BeanJob =
  ## Returns the delayed job with the shortest delay left.
  ## The job is not reserved by this operation!
  socket.send("peek-delayed\r\n")
  result = socket.recvFoundJob

proc peekBuried*(socket: Socket) : BeanJob =
  ## Returns the next job in the list of buried jobs.
  ## The job is not reserved by this operation!
  socket.send("peek-buried\r\n")
  result = socket.recvFoundJob

proc stats*(socket: Socket) : seq[string] =
  ## Gives statistical information about the system as a whole.
  ##
  ## A sequence of strings are returned with strings of "key: value".
  ## This may change to something more strongly types in the future.
  socket.send("stats\r\n")
  result = socket.recvStats

proc statsTube*(socket: Socket, tube: string) : seq[string] =
  ## Gives statistical information about the specified tube
  ## if it exists.
  ##
  ## A sequence of strings are returned with strings of "key: value".
  ## This may change to something more strongly types in the future.
  socket.send("stats-tube $#\r\n" % tube)
  result = socket.recvStats

proc statsJob*(socket: Socket, id: int) : seq[string] =
  ## Gives statistical information about the specified job if
  ## it exists.
  ##
  ## A sequence of strings are returned with strings of "key: value".
  ## This may change to something more strongly types in the future.
  socket.send("stats-job $#\r\n" % $id)
  result = socket.recvStats

proc quit*(socket: Socket) =
  ## Closes the connection.
  socket.send("quit\r\n")
