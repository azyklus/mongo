when not compileOption("threads"):
  {.error: "This module is available only when --threads:on".}

import std/locks
import std/net
import std/tables
import std/uri
import std/os
import std/streams
import std/md5
import std/strutils

import scram/client

import mongo/bson except `()`
import mongo/clientbase
import mongo/errors

type
  SharedChannel[T] = ptr Channel[T]
  SharedLock = ptr Lock

  Reader = SharedChannel[(int64, seq[Bson])]
  Writer = SharedChannel[string]
  Context = object
    reader: Reader
    writer: Writer
    replica: Replica
    info: SslInfo

  Mongo* = ref object of MongoBase    ## Mongo client object
    requestLock:                    SharedLock
    pool {.guard: requestLock.}:    seq[LockedSocket]
    threads:                        seq[Thread[Context]]
    current:                        int ##
    ## Current (possibly) free socket to use

  LockedSocket* = ref object of LockedSocketBase
    id:         int
    reader:     SharedChannel[(int64, seq[Bson])]
    writer:     SharedChannel[string]

proc newSharedChannel[T](): SharedChannel[T] =
  result = cast[SharedChannel[T]](allocShared0(sizeof(Channel[T])))
  open(result[])

proc close[T](ch: var SharedChannel[T]) =
  close(ch[])
  deallocShared(ch)
  ch = nil

proc newSharedLock(): SharedLock =
  result = cast[SharedLock](allocShared0(sizeof(Lock)))
  initLock(result[])

when false: # unused
  proc delSharedLock(l: var SharedLock) =
    deinitLock(l[])
    deallocShared(l)
    l = nil

template withRequestLock(body: untyped): untyped =
  m.requestLock[].acquire()
  try:
    {.locks: [m.requestLock].}:
      body
  finally:
    m.requestLock[].release()

proc newLockedSocket(id: int; connected = true): LockedSocket =
  ## Constructor for "locked" socket
  new result
  result.init()
  result.reader = newSharedChannel[(int64, seq[Bson])]()
  result.writer = newSharedChannel[string]()
  result.id = id
  result.connected = connected

proc handleResponses(c: Context) {.thread.}

proc initPool(m: var Mongo; maxConnections: int) =
  ## initialize the pool with maxConnections per replica
  m.threads = newSeq[Thread[Context]](maxConnections * m.replicas.len)
  m.requestLock = newSharedLock()
  withRequestLock:
    m.pool = newSeq[LockedSocket](m.threads.len)
    for i, pool in m.pool.mpairs:
      pool = newLockedSocket(i)
      let replica = m.replicas[i mod m.replicas.len]
      let context = Context(reader: pool.reader, writer: pool.writer,
                            info: m.info, replica: replica)
      createThread(m.threads[i], handleResponses, context)

proc newMongo*(url: Uri, maxConnections=16): Mongo =
  ## Mongo client constructor
  new result
  result.init url
  result.initPool maxConnections
  result.current = -1

proc newMongo*(host = "127.0.0.1"; port = mongoDefaultPort;
               secure=false, maxConnections=16): Mongo
  {.deprecated: "use a uri".} =
  var url: Uri
  url.hostname = host
  url.port = $port
  url.scheme =
    if secure: "mongodb+srv"
         else: "mongodb"
  result = newMongo(url, maxConnections = maxConnections)

proc newMongoWithURI*(u: Uri, maxConnections=16): Mongo
  {.deprecated: "use newMongo".} =
  newMongo(u, maxConnections = maxConnections)

proc newMongoWithURI*(u: string, maxConnections=16): Mongo
  {.deprecated: "use a uri".} =
  newMongoWithURI(parseUri(u), maxConnections)

proc authenticateScramSha1(db: Database[Mongo]; username, password: string;
                           ls: LockedSocket): bool {.discardable, gcsafe.}

proc acquire*(m: Mongo): LockedSocket =
  ## Retrieves next non-in-use socket for request
  while true:
    withRequestLock:
      for i in 0..m.pool.len():
        m.current = (m.current + 1) mod m.pool.len()
        let s = m.pool[m.current]
        if not s.inuse:
          if not s.authenticated and m.needsAuth:
            s.authenticated = m[m.authDb()].authenticateScramSha1(m.username, m.password, s)
            m.authenticated = s.authenticated
          s.inuse = true
          return s
    sleep(200)

proc release*(m: Mongo, ls: LockedSocket) =
  withRequestLock:
    if ls.inuse:
      ls.inuse = false
    else:
      raise newException(ValueError, "Socket can't be released twice")

proc connect*(m: Mongo): bool =
  ## Establish connection with Mongo server
  let s = m.acquire()
  m.release(s)
  result = s.connected

proc newMongoDatabase*(u: Uri): Database[Mongo] {.deprecated.} =
  ## Create new Mongo sync client using URI type
  let m = newMongoWithURI(u)
  if m.connect():
    result = m[u.path.extractFilename()]
    withRequestLock:
      for s in m.pool:
        s.authenticated =
          result.authenticateScramSha1(m.username, m.password, s)

proc consumeReply(reader: Reader; data: string) =
  ## consume a reply and send cursorId and bson documents to reader
  var stream = newStringStream(data)
  discard stream.readInt32()                     ## requestId
  discard stream.readInt32()                     ## responseTo
  discard stream.readInt32()                     ## opCode
  let responseFlags = stream.readInt32()         ## responseFlags
  var cursorId = stream.readInt64()              ## cursorID
  discard stream.readInt32()                     ## startingFrom
  let numberReturned = stream.readInt32()        ## numberReturned
  var res: seq[Bson]
  if (responseFlags and RFCursorNotFound) != 0:
    cursorId = 0
  if numberReturned > 0:
    for i in 0..<numberReturned:
      res.add newBsonDocument(stream)
  reader[].send((cursorId, res))

proc readExactly(sock: Socket; buffer: var string; want: Natural) =
  ## a read that's tolerant of incomplete recv() from the os
  var size = want
  var existing = buffer.len
  var data = newStringOfCap size
  while size > 0:
    let read = sock.recv(data, size)
    if read == 0:
      raise CommunicationError.newException:
        "server disconnected during read of $# bytes" % [ $size ]
    else:
      buffer.add data
      if read <= size:
        size -= read

  if want != buffer.len-existing:
    raise Defect.newException:
      "wanted $# but received only $#" %
        [ $want, $(buffer.len - existing) ]

proc handleResponses(c: Context) {.thread.} =
  ## this thread takes a Context to configure the connection, which also
  ## has the channels which we'll use to communicate with the client
  let sock = newSocket()

  # if the replica requires ssl,
  if c.replica.tls:
    # create the ssl context using our client-specific cert/key files,
    let ctx = setSsl c.info
    # and wrap the socket
    ctx.wrapSocket sock

  # connect to the replica
  sock.connect(c.replica.host, c.replica.port)

  var data: string  # prevent continuous reallocation of string buffers

  # service the connection until the client gives us an empty message
  while true:
    let pcktToSend = c.writer[].recv()
    if pcktToSend == "":
      break
    if not sock.trySend(pcktToSend):
      raise CommunicationError.newException:
        "unable to send packet to MongoDB server"

    # i guess we're just reading the reply length here
    data.setLen 0
    sock.readExactly(data, sizeof int32)

    # using stream's readInt32 to unpack an int32 🙄
    var stream = newStringStream data
    let messageLength = stream.readInt32() - sizeof(int32)
    data.setLen 0

    # now read the message body itself
    sock.readExactly(data, messageLength)

    # deliver the response to the reader
    consumeReply(c.reader, data)

proc refresh*(f: Cursor[Mongo], lockedSocket: LockedSocket = nil): seq[Bson] =
  ## Private procedure for performing actual query to Mongo
  if f.isClosed():
    raise CommunicationError.newException:
      "Cursor can't be closed while requesting"

  var res: string
  let numberToReturn = calcReturnSize(f)
  if f.isClosed():
    return @[]

  let reqID = f.connection().nextRequestId()
  if f.cursorId == 0:
    res = prepareQuery(f, reqID, numberToReturn, f.nskip)
  else:
    res = prepareMore(f, reqID, numberToReturn)

  var ls = lockedSocket
  if ls.isNil:
    ls = f.connection().acquire()
  ls.writer[].send(res)
  let (cursorId, data) = ls.reader[].recv()
  if lockedSocket.isNil:
    f.connection().release(ls)
  if f.cursorId == 0 or (f.queryFlags and TailableCursor) == 0:
    f.cursorId = cursorId
    if cursorId == 0:
      f.close()
  if data.len > 0:
    f.updateCount(data.len.int32)
    for doc in data:
      if doc.contains("$err"):
        if doc["code"].toInt == 50:
          raise OperationTimeout.newException:
            "Command " & $f & " has timed out"
  elif data.len == 0 and numberToReturn == 1:
    raise NotFound.newException "No documents matching query were found"
  else:
    discard
  return data

proc one(f: Cursor[Mongo], ls: LockedSocket): Bson =
  # Internal proc used for sending authentication requests on particular socket
  let docs = f.limit(1).refresh(ls)
  if docs.len == 0:
    raise NotFound.newException "No documents matching query were found"
  return docs[0]

proc authenticateScramSha1(db: Database[Mongo]; username, password: string;
                           ls: LockedSocket): bool {.discardable, gcsafe.} =
  ## Authenticate connection (sync): using SCRAM-SHA-1 auth method
  if username == "" or password == "":
    return false

  var scramClient = newScramClient[SHA1Digest]()
  let clientFirstMessage = scramClient.prepareFirstMessage(username)

  let requestStart = %*{
    "saslStart": 1'i32,
    "mechanism": "SCRAM-SHA-1",
    "payload": bin(clientFirstMessage),
    "autoAuthorize": 1'i32,
    "options": {
      "skipEmptyExchange": true,
    },
  }
  let responseStart = db["$cmd"].makeQuery(requestStart).one(ls)

  ## line to check if connect worked
  if responseStart.isNil or not responseStart["code"].isNil:
    return false #connect failed or auth failure
  db.client.authenticated = true
  let
    responsePayload = binstr(responseStart["payload"])
    passwordDigest = $toMd5("$#:mongo:$#" % [username, password])
    clientFinalMessage =
      scramClient.prepareFinalMessage(passwordDigest, responsePayload)
    requestContinue1 = %*{
      "saslContinue": 1'i32,
      "conversationId": toInt32(responseStart["conversationId"]),
      "payload": bin(clientFinalMessage)
    }
    responseContinue1 =  db["$cmd"].makeQuery(requestContinue1).one(ls)

  if responseContinue1["ok"].toFloat64() == 0.0:
    db.client.authenticated = false
    return false

  let payload = binstr(responseContinue1["payload"])
  if not scramClient.verifyServerFinalMessage(payload):
    raise MongoError.newException "Server returned an invalid signature."

  # Depending on how it's configured, Cyrus SASL (which the server uses)
  # requires a third empty challenge.
  if not responseContinue1["done"].toBool:
    let requestContinue2 = %*{
      "saslContinue": 1'i32,
      "conversationId": responseContinue1["conversationId"],
      "payload": ""
    }
    let responseContinue2 = db["$cmd"].makeQuery(requestContinue2).one(ls)
    if not responseContinue2["done"].toBool:
      raise MongoError.newException "SASL conversation failed to complete."
  return true
