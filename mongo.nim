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

proc find*(c: Collection[Mongo], filter: Bson, fields: seq[string] = @[], maxTime: int32 = 0): Cursor[Mongo] =
  ## Find query
  result = c.makeQuery(
    %*{
      "$query": filter
    },
    fields,
    maxTime
  )

# === Find API === #

proc all*(f: Cursor[Mongo]): seq[Bson] =
  ## Perform MongoDB query and return all matching documents
  while not f.isClosed():
    result.add(f.refresh())

proc one*(f: Cursor[Mongo]): Bson =
  ## Perform MongoDB query and return first matching document
  let docs = f.limit(1).refresh()
  if docs.len == 0:
    raise newException(NotFound, "No documents matching query were found")
  return docs[0]

proc oneOrNone*(f: Cursor[Mongo]): Bson =
  ## Perform MongoDB query and return first matching document or
  ## nil if not found.
  let docs = f.limit(1).refresh()
  if docs.len > 0:
    result = docs[0]

iterator items*(f: Cursor[Mongo]): Bson =
  ## Perform MongoDB query and return iterator for all matching documents
  while not f.isClosed():
    let docs = f.refresh()
    for doc in docs:
      yield doc

proc next*(f: Cursor[Mongo]): seq[Bson] =
  ## Perform MongoDB query for next batch of documents
  return f.refresh()

proc isMaster*(sm: Mongo): bool =
  ## Perform query in order to check if connected Mongo instance is a master
  return sm["admin"]["$cmd"].makeQuery(%*{"isMaster": 1}).one()["ismaster"].toBool

proc listDatabases*(sm: Mongo): seq[string] =
  ## Return list of databases on the server
  let response = sm["admin"]["$cmd"].makeQuery(%*{"listDatabases": 1}).one()
  if response.isReplyOk:
    for db in response["databases"].items():
      result.add(db["name"].toString())

proc createCollection*(db: Database[Mongo], name: string, capped: bool = false, autoIndexId: bool = true, maxSize: int = 0, maxDocs: int = 0): StatusReply =
  ## Create collection inside database via sync connection
  var request = %*{"create": name}

  if capped: request["capped"] = capped.toBson()
  if autoIndexId: request["autoIndexId"] = true.toBson()
  if maxSize > 0: request["size"] = maxSize.toBson()
  if maxDocs > 0: request["max"] = maxDocs.toBson()

  let response = db["$cmd"].makeQuery(request).one()
  return response.toStatusReply

proc listCollections*(db: Database[Mongo], filter: Bson = %*{}): seq[string] =
  ## List collections inside specified database
  let response = db["$cmd"].makeQuery(%*{"listCollections": 1'i32}).one()
  if response.isReplyOk:
    for col in response["cursor"]["firstBatch"]:
      result.add(col["name"].toString)

proc rename*(c: Collection[Mongo], newName: string, dropTarget: bool = false): StatusReply =
  ## Rename collection
  let
    request = %*{
      "renameCollection": $c,
      "to": "$#.$#" % [c.db.name, newName],
      "dropTarget": dropTarget
    }
    response = c.db.client["admin"]["$cmd"].makeQuery(request).one()
  c.name = newName
  return response.toStatusReply

proc drop*(db: Database[Mongo]): bool =
  ## Drop database from server
  let response = db["$cmd"].makeQuery(%*{"dropDatabase": 1}).one()
  return response.isReplyOk

proc drop*(c: Collection[Mongo]): tuple[ok: bool, message: string] =
  ## Drop collection from database
  let response = c.db["$cmd"].makeQuery(%*{"drop": c.name}).one()
  let status = response.toStatusReply
  return (ok: status.ok, message: status.err)

proc stats*(c: Collection[Mongo]): Bson =
  return c.db["$cmd"].makeQuery(%*{"collStats": c.name}).one()

proc count*(c: Collection[Mongo]): int =
  ## Return number of documents in collection
  return c.db["$cmd"].makeQuery(%*{"count": c.name}).one().getReplyN

proc count*(f: Cursor[Mongo]): int =
  ## Return number of documents in find query result
  return f.collection.db["$cmd"].makeQuery(%*{"count": f.collection.name, "query": f.filter}).one().getReplyN

proc sort*(f: Cursor[Mongo], criteria: Bson): Cursor[Mongo] =
  ## Setup sorting criteria
  f.sorting = criteria
  return f

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
    response = f.collection.db["$cmd"].makeQuery(request).one()

  if response.isReplyOk:
    for item in response["values"].items():
      result.add(item.toString())

proc getLastError*(m: Mongo): StatusReply =
  ## Get last error happened in current connection
  let response = m["admin"]["$cmd"].makeQuery(%*{"getLastError": 1'i32}).one()
  return response.toStatusReply

# ============= #
# Insert API    #
# ============= #

proc insert*(c: Collection[Mongo], documents: seq[Bson], ordered: bool = true, writeConcern: Bson = nil): StatusReply =
  ## Insert several new documents into MongoDB using one request

  #
  # insert any missing _id fields
  #
  var inserted_ids: seq[Bson] = @[]
  for doc in documents:
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
      "writeConcern": if writeConcern == nil.Bson: c.writeConcern else: writeConcern
    }
    response = c.db["$cmd"].makeQuery(request).one()

  return response.toStatusReply(inserted_ids=inserted_ids)

proc insert*(c: Collection[Mongo], document: Bson, ordered: bool = true, writeConcern: Bson = nil): StatusReply =
  ## Insert new document into MongoDB via sync connection
  return c.insert(@[document], ordered, if writeConcern == nil.Bson: c.writeConcern else: writeConcern)

# =========== #
# Update API  #
# =========== #

proc update*(c: Collection[Mongo], selector: Bson, update: Bson, multi: bool, upsert: bool): StatusReply =
  ## Update MongoDB document[s]
  let
    request = %*{
      "update": c.name,
      "updates": [%*{"q": selector, "u": update, "upsert": upsert, "multi": multi}],
      "ordered": true
    }
    response = c.db["$cmd"].makeQuery(request).one()
  return response.toStatusReply

# ==================== #
# Find and modify API  #
# ==================== #

proc findAndModify*(c: Collection[Mongo], selector: Bson, sort: Bson, update: Bson, afterUpdate: bool, upsert: bool, writeConcern: Bson = nil, remove: bool = false): StatusReply =
  ## Finds and modifies MongoDB document
  let request = %*{
    "findAndModify": c.name,
    "query": selector,
    "new": afterUpdate,
    "upsert": upsert,
    "writeConcern": if writeConcern == nil.Bson: c.writeConcern else: writeConcern
  }
  if not sort.isNil:
    request["sort"] = sort
  if remove:
    request["remove"] = remove.toBson()
  else:
    request["update"] = update
  let response = c.db["$cmd"].makeQuery(request).one()
  return response.toStatusReply

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
    response = c.db["$cmd"].makeQuery(request).one()
  return response.toStatusReply

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
  let response = db["$cmd"].makeQuery(createUserRequest).one()
  return response.isReplyOk

proc dropUser*(db: Database[Mongo], username: string): bool =
  ## Drop user from the db
  let
    dropUserRequest = %*{
      "dropUser": username,
      "writeConcern": db.client.writeConcern
      }
    response = db["$cmd"].makeQuery(dropUserRequest).one()
  return response.isReplyOk

# ============== #
# Authentication #
# ============== #

proc authenticate*(db: Database[Mongo], username: string, password: string): bool =
  ## Authenticate connection (sync): using MONGODB-CR auth method
  if username == "" or password == "":
    return false

  let nonce = db["$cmd"].makeQuery(%*{"getnonce": 1'i32}).one()["nonce"].toString
  let passwordDigest = $toMd5("$#:mongo:$#" % [username, password])
  let key = $toMd5("$#$#$#" % [nonce, username, passwordDigest])
  let request = %*{
    "authenticate": 1'i32,
    "mechanism": "MONGODB-CR",
    "user": username,
    "nonce": nonce,
    "key": key,
    "autoAuthorize": 1'i32
  }
  let response = db["$cmd"].makeQuery(request).one()
  return response.isReplyOk
