import std/os
import std/strutils
import std/uri
import std/net

when defined(ssl):
  import std/openssl
  import pkg/dnsclient

import mongo/bson except `()`
import mongo/writeconcern
import mongo/proto
import mongo/errors

when compileOption("threads"):
  import std/locks

const
  mongoTls* = defined(ssl)
  mongoVerifyPeer* {.booldefine.} = true
  mongoCaFile* {.strdefine, used.} = ""
  mongoDefaultHost* = "127.0.0.1"
  mongoDefaultPort* = 27017.Port ## Default MongoDB IP Port
  mongoSslProtocol* {.strdefine, used.} = ""

  TailableCursor*  = 1'i32 shl 1 ## Leave cursor alive on MongoDB side
  SlaveOk*         = 1'i32 shl 2 ## Allow to query replica set slaves
  NoCursorTimeout* = 1'i32 shl 4 ##
  AwaitData*       = 1'i32 shl 5 ##
  Exhaust*         = 1'i32 shl 6 ##
  Partial*         = 1'i32 shl 7 ## Get info only from running shards

  RFCursorNotFound*    = 1'i32       ##
  ## CursorNotFound. Is set when getMore is called but the cursor id is
  ## not valid at the server. Returned with zero results.

  RFQueryFailure*      = 1'i32 shl 1 ##
  ## QueryFailure. Is set when query failed. Results consist of one
  ## document containing an “$err” field describing the failure.

  RFShardConfigStale*  = 1'i32 shl 2 ##
  ## ShardConfigStale. Drivers should ignore this. Only mongos will ever
  ## see this set, in which case, it needs to update config from the
  ## server.

  RFAwaitCapable*      = 1'i32 shl 3 ##
  ## AwaitCapable. Is set when the server supports the AwaitData Query
  ## option. If it doesn’t, a client should sleep a little between
  ## getMore’s of a Tailable cursor. Mongod version 1.6 supports
  ## AwaitData and thus always sets AwaitCapable.

type
  SslInfo* = object
    ## SslInfo will handle information for connecting with SSL/TLS
    ## connection.
    keyfile*: string            ## Key file path
    certfile*: string           ## Certificate file path
    when defined(ssl):
      protocol*: SslProtVersion ## The SSL/TLS protocol

  Replica* = object
    host*: string
    port*: Port
    tls*: bool

  MongoBase* = ref object of RootObj ## Base for Mongo clients
    when compileOption("threads"):
      reqIdLock:  Lock
      requestId {.guard: reqIdLock.}: int32
    else:
      requestId:    int32
    queryFlags: int32
    username: string
    password: string
    db: string
    needAuth: bool
    authenticated: bool
    replicas: seq[Replica]
    writeConcern: WriteConcern
    info: SslInfo

  Database*[T] = ref DatabaseObj[T]
  DatabaseObj*[T] = object ## MongoDB database object
    name:   string
    client*: T

  Collection*[T] = ref CollectionObj[T]
  CollectionObj*[T] = object ## MongoDB collection object
    name: string
    db*: Database[T]
    client: T

  CollectionInfo* = ref CollectionInfoObj
  CollectionInfoObj* = object  ## Collection information (for manual creation)
    disableIdIndex*: bool
    forceIdIndex*: bool
    capped: bool
    maxBytes: int
    maxDocs: int

  Cursor*[T] = ref CursorObj[T]
  CursorObj*[T] = object     ## MongoDB cursor: manages queries object lazily
    collection: Collection[T]
    query: Bson
    fields: seq[string]
    queryFlags*: int32
    nskip: int32
    nlimit: int32
    nbatchSize: int32
    sorting*: Bson
    cursorId: int64
    count: int32
    closed: bool

  LockedSocketBase* {.inheritable.} = ref LockedSocketBaseObj
  LockedSocketBaseObj* {.inheritable.} = object
    inuse:         bool
    authenticated: bool
    connected:     bool

template lockIfThreads(body: untyped): untyped =
  when compileOption("threads"):
    mb.reqIdLock.acquire()
    try:
      {.locks: [mb.reqIdLock].}:
        body
    finally:
      mb.reqIdLock.release()
  else:
    body

proc setSsl*(sslinfo: SslInfo): SslContext =
  ## Create (and maybe validate) an SSL context with cert/key files.
  when defined(ssl):
    # honor an override of the cert file
    template cafile: string =
      when mongoCaFile == "": sslinfo.certfile
      else: mongoCaFile
    let mode =
      when mongoVerifyPeer:                            # user wants verify
        when defined(nimDisableCertificateValidation): # nim says `no`
          {.warning: "nim disabled certificate validation".}
        CVerifyPeerUseEnvVars
      else:
        CVerifyNone

    # instantiating the context will also perform certificate verification
    let c = newContext(protVersion = sslinfo.protocol, verifyMode = mode,
                       certfile = cafile, keyfile = sslinfo.keyfile)
    if c.isNil:
      raise ValueError.newException "unable to initialize ssl context"
    else:
      result = c

when defined(ssl):
  proc initSslInfo*(keyfile, certfile: string, prot = protSSLv23): SSLInfo =
    ## Init the SSLinfo which give default value of protocol to
    ## protSSLv23. It's preferable used when user want to use
    ## SSL/TLS connection.
    SSLInfo(keyfile: keyfile, certfile: certfile, protocol: prot)

method initSslInfo(m: MongoBase): SSLInfo {.base.} =
  ## Prepare SSLInfo for setSsl().
  when defined(ssl):
    let prot =
      if mongoSslProtocol == "":
        protSSLv23
      else:
        parseEnum[SslProtVersion](mongoSslProtocol)
    result = initSslInfo("", "", prot)

  # we don't have any of this option infrastructure
  when false:
    proc setCertKey (s: var SslInfo, vals: seq[string]) =
      for kv in vals:
        let kvs = kv.split ':'
        if kvs[0].toLower == "certificate":
          s.certfile = decodeUrl kvs[1]
        elif kvs[0].toLower == "key":
          s.keyfile = decodeUrl kvs[1]

    if "tlsCertificateKeyFile".toLower in tbl:
      result.setCertKey tbl["tlsCertificateKeyFile".toLower]

method `sslInfo=`*(m: MongoBase; info: SSLInfo) {.base.} =
  m.info = info

method checkTlsValidity*(m: MongoBase) {.base.} =
  # we don't do any of this yet
  when false:
    let tlsCertInval = ["tlsInsecure", "tlsAllowInvalidCertificates"]
    let tlsHostInval = ["tlsInsecure", "tlsAllowInvalidHostnames"]
    if tlsCertInval.allIt(it.toLower in m.query):
      raise MongoError.newException:
        "Can't have " & tlsCertInval.join(" and ")
    if tlsHostInval.allIt(it.toLower in m.query):
      raise MongoError.newException:
        "Can't have " & tlsHostInval.join(" and ")

method init*(mb: MongoBase) {.base.} =
  lockIfThreads:
    mb.requestID = 0
  mb.queryFlags = 0
  mb.replicas.setLen 0
  mb.username = ""
  mb.password = ""
  mb.db = "admin"
  mb.needAuth = false
  mb.authenticated = false
  mb.writeConcern = writeConcernDefault()

method init*(mb: MongoBase; replicas: openArray[Replica]) {.base.} =
  init mb
  mb.replicas.setLen replicas.len
  for i, replica in mb.replicas.mpairs:  # workaround nim bug
    replica = replicas[i]

proc parsePort(url: Uri): Port =
  if url.port.len > 0:
    parseInt(url.port).Port
  else:
    mongoDefaultPort

method init*(mb: MongoBase; url: Uri) {.base.} =
  const schemes =
    when defined(ssl):
      ["mongodb", "mongo", "mongodb+srv", "mongo+srv"]
    else:
      ["mongodb", "mongo"]

  if url.scheme notin schemes:
    raise MongoError.newException:
      "unsupported scheme `$#`; expected ($#)" %
        [ url.scheme, schemes.join("|") ]

  var replicas: seq[Replica]
  block:
    when mongoTls:
      if "+srv" in url.scheme:
        # populate the replicas by retrieving SRV records from DNS
        try:
          let client = newDNSClient(getEnv("DNS_SERVER", "8.8.8.8"))
          let reply = client.sendQuery("_mongodb._tcp." & url.hostname, SRV)
          for answer in reply.answers.items:
            let record = SRVRecord answer
            replicas.add Replica(host: record.target, tls: mongoTls,
                                 port: record.port.Port)
        except TimeoutError as e:
          raise MongoError.newException "DNS timeout: " & e.msg
        break
    # use the provided hostname and port as the sole replica (no tls)
    replicas.add Replica(host: url.hostname, port: parsePort url)

  if replicas.len == 0:
    raise MongoError.newException:
      "unable to find replicas for `$#`" % [ url.hostname ]

  mb.init replicas
  mb.username = url.username
  mb.password = url.password
  let db = url.path.extractFilename()
  if db != "":
    mb.db = db
  mb.needAuth = (mb.username != "" and mb.db != "")
  mb.sslInfo = initSslInfo mb

proc info*(mb: MongoBase): SslInfo =
  ## Retrieve the cert/key files and SSL protocol selection.
  mb.info

proc replicas*(mb: MongoBase): seq[Replica] =
  ## Available replicas
  mb.replicas

proc host*(mb: MongoBase): string {.deprecated.} =
  ## Connected server host
  mb.replicas[0].host

proc port*(mb: MongoBase): uint16 {.deprecated.} =
  ## Connected server port
  mb.replicas[0].port.uint16

proc username*(mb: MongoBase): string =
  ## Username to authenticate at Mongo Server
  mb.username

proc password*(mb: MongoBase): string =
  ## Password to authenticate at Mongo Server
  mb.password

proc authDb*(mb: MongoBase): string =
  ## Database for authentication
  mb.db

proc needsAuth*(mb: MongoBase): bool =
  ## Check if connection needs to be authenticated
  mb.needAuth

proc queryFlags*(mb: MongoBase): int32 =
  ## Query flags perform query flow and connection settings
  mb.queryFlags

proc `queryFlags=`*(mb: MongoBase, flags: int32) =
  ## Query flags perform query flow and connection settings
  mb.queryFlags = flags

proc nextRequestId*(mb: MongoBase): int32 =
  ## Return next request id for current MongoDB client
  lockIfThreads:
    mb.requestId = (mb.requestId + 1) mod (int32.high - 1'i32)
    result = mb.requestId

proc writeConcern*(mb: MongoBase): WriteConcern =
  ## Getter for currently setup client's write concern
  mb.writeConcern

proc `writeConcern=`*(mb: MongoBase, concern: WriteConcern) =
  ## Set client-wide write concern for sync client
  assert "w" in concern
  mb.writeConcern = concern

proc authenticated*(mb: MongoBase): bool =
  ## Query authenticated flag
  mb.authenticated

proc `authenticated=`*(mb: MongoBase, authenticated: bool) =
  ## Enable/disable authenticated flag for database
  mb.authenticated = authenticated

proc tailableCursor*(m: MongoBase, enable: bool = true): MongoBase =
  ## Enable/disable tailable behaviour for the cursor (cursor is not
  ## removed immediately after the query)
  result = m
  m.queryFlags =
    if enable:
      m.queryFlags or TailableCursor
    else:
      m.queryFlags and (not TailableCursor)

proc slaveOk*(m: MongoBase, enable: bool = true): MongoBase =
  ## Enable/disable querying from slaves in replica sets
  result = m
  m.queryFlags =
    if enable:
      m.queryFlags or SlaveOk
    else:
      m.queryFlags and (not SlaveOk)

proc noCursorTimeout*(m: MongoBase, enable: bool = true): MongoBase =
  ## Enable/disable cursor idle timeout
  result = m
  m.queryFlags =
    if enable:
      m.queryFlags or NoCursorTimeout
    else:
      m.queryFlags and (not NoCursorTimeout)

proc awaitData*(m: MongoBase, enable: bool = true): MongoBase =
  ## Enable/disable data waiting behaviour (along with tailable cursor)
  result = m
  m.queryFlags =
    if enable:
      m.queryFlags or AwaitData
    else:
      m.queryFlags and (not AwaitData)

proc exhaust*(m: MongoBase, enable: bool = true): MongoBase =
  ## Enable/disabel exhaust flag which forces database to giveaway
  ## all data for the query in form of "get more" packages.
  result = m
  m.queryFlags =
    if enable:
      m.queryFlags or Exhaust
    else:
      m.queryFlags and (not Exhaust)

proc allowPartial*(m: MongoBase, enable: bool = true): MongoBase =
  ## Enable/disable allowance for partial data retrieval from mongos when
  ## one or more shards are down.
  result = m
  m.queryFlags =
    if enable:
      m.queryFlags or Partial
    else:
      m.queryFlags and (not Partial)

proc `$`*(m: MongoBase): string =
  ## Return full DSN for the Mongo connection
  var url: Uri
  url.hostname = m.replicas[0].host
  url.port = $m.replicas[0].port
  url.scheme = if m.replicas[0].tls: "mongodb+srv" else: "mongodb"
  url.path = m.authDb
  url.username = m.username
  url.password = m.password
  result = $url

proc `[]`*[T: MongoBase](client: T, dbName: string): Database[T] =
  ## Retrieves database from Mongo
  result.new()
  result.name = dbName
  result.client = client

# === Locked Sockets API === #

method init*(ls: LockedSocketBase) {.base.} =
  ls.inuse = false
  ls.authenticated = false
  ls.connected = false

proc inuse*(ls: LockedSocketBase): bool =
  ## Return inuse
  ls.inuse

proc `inuse=`*(ls: LockedSocketBase, inuse: bool) =
  ## Enable/disable inuse flag for socket
  ls.inuse = inuse

proc authenticated*(ls: LockedSocketBase): bool =
  ## Return authenticated
  ls.authenticated

proc `authenticated=`*(ls: LockedSocketBase, authenticated: bool) =
  ## Enable/disable authenticated flag for socket
  ls.authenticated = authenticated

proc connected*(ls: LockedSocketBase): bool =
  ## Return connected
  ls.connected

proc `connected=`*(ls: LockedSocketBase, connected: bool) =
  ## Enable/disable connected flag for socket
  ls.connected = connected

# === Database API === #

proc `$`*(db: Database): string =
  ## Database name string representation
  return db.name

proc `[]`*[T: MongoBase](db: Database[T];
                         collectionName: string): Collection[T] =
  ## Retrieves collection from Mongo Database
  result.new()
  result.name = collectionName
  result.client = db.client
  result.db = db

proc name*(db: Database): string = db.name
  ## Return name of database

proc `name=`*(db: Database, name: string) =
  ## Set new database name
  db.name = name

# === Collection API === #

proc `$`*(c: Collection): string =
  ## String representation of collection name
  return c.db.name & "." & c.name

proc newCursor[T](c: Collection[T]): Cursor[T] =
  ## Private constructor for the Find object. Find acts by taking
  ## client settings (flags) that can be overriden when actual
  ## query is performed.
  result.new()
  result.collection = c
  result.fields = @[]
  result.queryFlags = c.client.queryFlags
  result.nskip = 0
  result.nlimit = 0
  result.nbatchSize = 0
  result.cursorId = 0
  result.count = 0
  result.closed = false

proc makeQuery*[T: MongoBase](c: Collection[T]; query: Bson;
                              fields: seq[string] = @[];
                              maxTime: int32 = 0): Cursor[T] =
  ## Create lazy query object to MongoDB that can be actually run
  ## by one of the Find object procedures: `one()` or `all()`.
  result = c.newCursor()
  result.query = query
  result.fields = fields
  if maxTime > 0:
    result.query["$maxTimeMS"] = maxTime.toBson()


proc db*[T: MongoBase](c: Collection[T]): Database[T] =
  ## Return the database from collection
  c.db

proc name*(c: Collection): string =
  ## Return name of collection
  c.name

proc `name=`*(c: Collection, name: string) =
  ## Set new collection name
  c.name = name

proc writeConcern*(c: Collection): WriteConcern =
  ## Return write concern for collection
  c.client.writeConcern

# === Find API === #

proc prepareQuery*(f: Cursor; requestId: int32; numberToReturn: int32;
                   numberToSkip: int32): string =
  ## Prepare query and request queries for making OP_QUERY
  var bfields: Bson = newBsonDocument()
  if f.fields.len() > 0:
    for field in f.fields.items():
      bfields[field] = 1'i32.toBson()
  let squery = f.query.bytes()
  let sfields: string = if f.fields.len() > 0: bfields.bytes() else: ""
  let colName = $(f.collection)
  result = ""
  var msg = ""
  buildMessageQuery(f.queryFlags, colName, numberToSkip, numberToReturn, msg)
  msg &= squery
  msg &= sfields
  buildMessageHeader(msg.len().int32, requestId, 0, OP_QUERY, result)
  result &= msg

proc prepareMore*(f: Cursor, requestId: int32, numberToReturn: int32): string =
  ## Prepare query and request queries for making OP_GET_MORE
  let colName = $(f.collection)
  result = ""
  var msg = ""
  buildMessageMore(colName, f.cursorId, numberToReturn, msg)
  buildMessageHeader(msg.len().int32, requestId, 0, OP_GET_MORE, result)
  result &= msg

proc orderBy*(f: Cursor, order: Bson): Cursor =
  ## Add sorting setting to query
  result = f
  f.query["$orderby"] = order

proc tailableCursor*(f: Cursor, enable: bool = true): Cursor =
  ## Enable/disable tailable behaviour for the cursor (cursor is not
  ## removed immediately after the query)
  result = f
  f.queryFlags =
    if enable:
      f.queryFlags or TailableCursor
    else:
      f.queryFlags and (not TailableCursor)

proc slaveOk*(f: Cursor, enable: bool = true): Cursor =
  ## Enable/disable querying from slaves in replica sets
  result = f
  f.queryFlags =
    if enable:
      f.queryFlags or SlaveOk
    else:
      f.queryFlags and (not SlaveOk)

proc noCursorTimeout*(f: Cursor, enable: bool = true): Cursor =
  ## Enable/disable cursor idle timeout
  result = f
  f.queryFlags =
    if enable:
      f.queryFlags or NoCursorTimeout
    else:
      f.queryFlags and (not NoCursorTimeout)

proc awaitData*(f: Cursor, enable: bool = true): Cursor =
  ## Enable/disable data waiting behaviour (along with tailable cursor)
  result = f
  f.queryFlags =
    if enable:
      f.queryFlags or AwaitData
    else:
      f.queryFlags and (not AwaitData)

proc exhaust*(f: Cursor, enable: bool = true): Cursor =
  ## Enable/disabel exhaust flag which forces database to giveaway
  ## all data for the query in form of "get more" packages.
  result = f
  f.queryFlags =
    if enable:
      f.queryFlags or Exhaust
    else:
      f.queryFlags and (not Exhaust)

proc allowPartial*(f: Cursor, enable: bool = true): Cursor =
  ## Enable/disable allowance for partial data retrieval from mongo when
  ## on or more shards are down.
  result = f
  f.queryFlags =
    if enable:
      f.queryFlags or Partial
    else:
      f.queryFlags and (not Partial)

proc skip*(f: Cursor, numSkip: int32): Cursor =
  ## Specify number of documents from return sequence to skip
  result = f
  result.nskip = numSkip

proc limit*(f: Cursor, numLimit: int32): Cursor =
  ## Specify number of documents to return from database
  result = f
  # Should be negative if hard limit, else soft limit used
  result.nlimit = numLimit

proc batchSize*(f: Cursor, numBatchSize: int32): Cursor =
  ## Specify number of documents in first reply. Conflicts with limit
  result = f
  result.nbatchSize = numBatchSize

proc calcReturnSize*(f: Cursor): int32 =
  if f.nlimit == 0:
    result = f.nbatchSize
  elif f.nlimit < 0:
    result = f.nlimit
  else:
    result = f.nlimit - f.count
    if result <= 0:
      f.closed = true
      # TODO Add kill cursor functionality here
    if f.nbatchSize > 0:
      result = min(result, f.nbatchSize).int32

proc updateCount*(f: Cursor, count: int32) =
  ## Increasing the count of returned documents
  f.count += count

proc isClosed*(f: Cursor): bool =
  ## Return status of cursor
  f.closed

proc close*(f: Cursor) =
  ## Close cursor
  f.closed = true

proc `$`*(f: Cursor): string =
  ## Return query of cursor as a string
  $f.query

proc connection*[T: MongoBase](f: Cursor[T]): T =
  ## Get connection of cursor
  f.collection.client

proc collection*[T: MongoBase](f: Cursor[T]): Collection[T] =
  ## Get collection from cursor
  f.collection

proc cursorId*(f: Cursor): int64 =
  ## Return cursor ID
  f.cursorId

proc `cursorId=`*(f: Cursor, cursorId: int64) =
  ## Set cursor ID
  f.cursorId = cursorId

proc nskip*(f: Cursor): int32 =
  ## Return amount of documents to skip
  f.nskip

proc filter*(f: Cursor): Bson =
  ## Return filter of query from cursor
  f.query["$query"]
