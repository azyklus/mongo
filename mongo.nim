import std/options
import std/md5
import std/net
import std/oids
import std/strutils

import mongo/bson except `()`
export bson

import mongo/auth
import mongo/clientbase
import mongo/errors
import mongo/reply
import mongo/writeconcern

when compileOption("threads"):
  import mongo/threaded as sync
else:
  import mongo/single as sync

export auth
export clientbase except nextRequestId, init, calcReturnSize, updateCount, makeQuery, prepareMore, prepareQuery
export errors
export reply
export writeconcern
export sync except acquire, release, refresh

# === Collection API === #

proc find*(c: Collection[Mongo]; filter: Bson; fields: seq[string] = @[];
           maxTime = 0'i32): Cursor[Mongo] =
  ## Find query
  c.makeQuery(%*{ "$query": filter }, fields, maxTime)

# === Find API === #

iterator items*(f: Cursor[Mongo]): Bson =
  ## Perform MongoDB query and return iterator for all matching documents
  while not f.isClosed:
    for doc in f.refresh:
      yield doc

proc next*(f: Cursor[Mongo]): seq[Bson] =
  ## Perform MongoDB query and return sequence of all matching documents
  for doc in f.refresh:
    result.add doc

proc first*(f: Cursor[Mongo]): Bson =
  ## Perform MongoDB query and return first matching document
  for doc in f.limit(1).items:
    return doc
  raise NotFound.newException "No documents matching query were found"

proc firstOrNone*(f: Cursor[Mongo]): Option[Bson] =
  ## Perform MongoDB query and return first matching document, if available.
  try:
    result = some f.first
  except NotFound:
    discard

proc isMaster*(sm: Mongo): bool =
  ## Perform query in order to check if connected Mongo instance is a master
  let doc = sm["admin"]["$cmd"].makeQuery(%*{"isMaster": 1}).first
  result = doc["ismaster"].toBool

iterator keys*(sm: Mongo): string =
  ## Iterate over databases on the server
  let response = sm["admin"]["$cmd"].makeQuery(%*{"listDatabases": 1}).first
  let status = response.toStatusReply
  if status.ok:
    for b in response["databases"].items:
      yield b["name"].toString
  else:
    raise MongoError.newException:
      "unable to fetch databases: " & status.err

iterator pairs*(sm: Mongo): tuple[name: string, database: Database[Mongo]] =
  ## Iterate over databases on the server
  for name in sm.keys:
    yield (name: name, database: sm[name])

iterator values*(sm: Mongo): Database[Mongo] =
  ## Iterate over databases on the server
  for _, db in sm.pairs:
    yield db

proc createCollection*(db: Database[Mongo]; name: string; capped = false;
                       autoIndexId = true; maxSize = 0;
                       maxDocs = 0): StatusReply =
  ## Create collection inside database via sync connection
  var request = %*{"create": name}

  if capped: request["capped"] = capped.toBson
  if autoIndexId: request["autoIndexId"] = true.toBson
  if maxSize > 0: request["size"] = maxSize.toBson
  if maxDocs > 0: request["max"] = maxDocs.toBson

  let response = db["$cmd"].makeQuery(request).first
  return response.toStatusReply

iterator keys*(db: Database[Mongo]; filter: Bson = %*{}): string =
  ## List collections inside specified database
  let response = db["$cmd"].makeQuery(%*{"listCollections": 1'i32}).first
  let status = response.toStatusReply
  if status.ok:
    for col in response["cursor"]["firstBatch"]:
      yield col["name"].toString
  else:
    raise MongoError.newException:
      "unable to fetch collections: " & status.err

iterator pairs*(db: Database[Mongo]; filter: Bson = %*{}):
  tuple[name: string, collection: Collection[Mongo]] =
  ## List collections inside specified database
  for name in db.keys(filter = filter):
    yield (name: name, collection: db[name])

iterator values*(db: Database[Mongo]; filter: Bson = %*{}): Collection[Mongo] =
  ## List collections inside specified database
  for _, col in db.pairs(filter = filter):
    yield col

proc rename*(c: Collection[Mongo]; name: string; dropTarget = false): StatusReply =
  ## Rename collection
  let
    request = %*{
      "renameCollection": $c,
      "to": "$#.$#" % [c.db.name, name],
      "dropTarget": dropTarget
    }
    response = c.db.client["admin"]["$cmd"].makeQuery(request).first
  c.name = name
  result = response.toStatusReply

proc drop*(db: Database[Mongo]): bool =
  ## Drop database from server
  db["$cmd"].makeQuery(%*{"dropDatabase": 1}).first.isReplyOk

proc drop*(c: Collection[Mongo]): bool =
  ## Drop collection from database; returns `true` if the database existed,
  ## `false` otherwise.
  let status = c.db["$cmd"].makeQuery(%*{"drop": c.name}).first.toStatusReply
  if not status.ok:
    if "ns not found" in status.err:
      result = false
    else:
      raise MongoError.newException "collection drop: " & status.err
  else:
    result = true

proc stats*(c: Collection[Mongo]): Bson =
  c.db["$cmd"].makeQuery(%*{"collStats": c.name}).first

proc len*(c: Collection[Mongo]): int =
  ## Return number of documents in collection
  c.db["$cmd"].makeQuery(%*{"count": c.name}).first.getReplyN

proc len*(f: Cursor[Mongo]): int =
  ## Return number of documents in find query result
  f.collection.db["$cmd"].makeQuery(%*{"count": f.collection.name,
                                       "query": f.filter}).first.getReplyN

proc sort*(f: Cursor[Mongo]; criteria: Bson) =
  ## Setup sorting criteria
  f.sorting = criteria

proc unique*(f: Cursor[Mongo], key: string): seq[string] =
  ## Force cursor to return only distinct documents by specified field.
  ## Corresponds to '.distinct()' MongoDB command. If Nim we use 'unique'
  ## because 'distinct' is Nim's reserved keyword.
  let
    request = %*{
      "distinct": f.collection.name,
      "query": f.filter,
      "key": key
    }
    response = f.collection.db["$cmd"].makeQuery(request).first

  if response.isReplyOk:
    for item in response["values"].items:
      result.add item.toString

proc getLastError*(m: Mongo): StatusReply =
  ## Get last error happened in current connection
  m["admin"]["$cmd"].makeQuery(%*{"getLastError": 1'i32}).first.toStatusReply

# ============= #
# Insert API    #
# ============= #

proc insert*(c: Collection[Mongo]; documents: seq[Bson];
             ordered = true; writeConcern: Bson = nil): StatusReply =
  ## Insert several new documents into MongoDB using one request

  #
  # insert any missing _id fields
  #
  var inserted_ids: seq[Bson] = @[]
  for doc in documents.items:
    if not doc.contains("_id"):
      doc["_id"] = toBson(genOid())
    inserted_ids.add(doc["_id"])

  #
  # build & send Mongo query
  #
  let
    request = %*{
      "insert": c.name,
      "documents": documents,
      "ordered": ordered,
      "writeConcern":
        if writeConcern == nil.Bson:
          c.writeConcern
        else:
          writeConcern
    }
    response = c.db["$cmd"].makeQuery(request).first
  result = response.toStatusReply(inserted_ids = inserted_ids)

proc insert*(c: Collection[Mongo]; document: Bson; ordered = true;
             writeConcern: Bson = nil): StatusReply =
  ## Insert new document into MongoDB via sync connection
  let wc =
    if writeConcern == nil.Bson:
      c.writeConcern
    else:
      writeConcern
  result = c.insert(@[document], ordered, writeConcern = wc)

# =========== #
# Update API  #
# =========== #

proc update*(c: Collection[Mongo]; selector, update: Bson;
             multi, upsert: bool): StatusReply =
  ## Update MongoDB document[s]
  let
    request = %*{
      "update": c.name,
      "updates": [%*{"q": selector, "u": update, "upsert": upsert, "multi": multi}],
      "ordered": true
    }
  result = c.db["$cmd"].makeQuery(request).first.toStatusReply

# ==================== #
# Find and modify API  #
# ==================== #

proc findAndModify*(c: Collection[Mongo]; selector, sort, update: Bson;
                    afterUpdate, upsert: bool; writeConcern: Bson = nil;
                    remove = false): StatusReply =
  ## Finds and modifies MongoDB document
  let request = %*{
    "findAndModify": c.name,
    "query": selector,
    "new": afterUpdate,
    "upsert": upsert,
    "writeConcern":
      if writeConcern == nil.Bson:
        c.writeConcern
      else:
        writeConcern
  }
  if not sort.isNil:
    request["sort"] = sort
  if remove:
    request["remove"] = remove.toBson()
  else:
    request["update"] = update
  result = c.db["$cmd"].makeQuery(request).first.toStatusReply

# ============ #
# Remove API   #
# ============ #

proc remove*(c: Collection[Mongo], selector: Bson, limit: int = 0, ordered: bool = true, writeConcern: Bson = nil): StatusReply =
  ## Delete document[s] from MongoDB
  let
    request = %*{
      "delete": c.name,
      "deletes": [%*{"q": selector, "limit": limit}],
      "ordered": true,
      "writeConcern": if writeConcern == nil.Bson: c.writeConcern else: writeConcern
    }
  result = c.db["$cmd"].makeQuery(request).first.toStatusReply

# =============== #
# User management
# =============== #

proc createUser*(db: DataBase[Mongo], username: string, pwd: string, customData: Bson = newBsonDocument(), roles: Bson = newBsonArray()): bool =
  ## Create new user for the specified database
  let createUserRequest = %*{
    "createUser": username,
    "pwd": pwd,
    "customData": customData,
    "roles": roles,
    "writeConcern": db.client.writeConcern
  }
  result = db["$cmd"].makeQuery(createUserRequest).first.isReplyOk

proc dropUser*(db: Database[Mongo], username: string): bool =
  ## Drop user from the db
  let
    dropUserRequest = %*{
      "dropUser": username,
      "writeConcern": db.client.writeConcern
      }
  result = db["$cmd"].makeQuery(dropUserRequest).first.isReplyOk

# ============== #
# Authentication #
# ============== #

proc authenticate*(db: Database[Mongo]; user, pass: string): bool =
  ## Authenticate connection (sync): using MONGODB-CR auth method
  if user == "" or pass == "":
    return false

  let nonce =
    db["$cmd"].makeQuery(%*{"getnonce": 1'i32}).first["nonce"].toString
  let passwordDigest = $toMd5("$#:mongo:$#" % [user, pass])
  let key = $toMd5("$#$#$#" % [nonce, user, passwordDigest])
  let request = %*{
    "authenticate": 1'i32,
    "mechanism": "MONGODB-CR",
    "user": user,
    "nonce": nonce,
    "key": key,
    "autoAuthorize": 1'i32
  }
  result = db["$cmd"].makeQuery(request).first.isReplyOk
