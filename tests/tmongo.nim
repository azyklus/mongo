import std/sequtils
import std/oids
import std/strutils
import std/times
import std/os

import balls

import mongo

template checkOk(s: StatusReply): untyped =
  check s.ok, s.err & "\n" & $s.bson

const
  TestDB   = "testdb"
  TestCol  = "testcol"

var
  sm: Mongo = newMongo(maxConnections=2)           ## Mongo synchronous client

let
  sdb: Database[Mongo] = sm[TestDB]
  sco: Collection[Mongo] = sdb[TestCol]

check sm.connect()

suite "Mongo instance administration commands test suite":
  test "Init":
    check:
      sm.writeConcern["w"].toInt32() == writeConcernDefault()["w"].toInt32()

  test "Command: 'isMaster'":
    discard sm.isMaster()

  test "Command: 'dropDatabase'":
    check drop sdb

  test "Command: 'listDatabases'":
    checkOk sco.insert(%*{"test": "test"})
    check "testdb" in sm.keys.toSeq
    checkOk sco.remove(%*{"test": "test"}, limit=1)

  test "Command: 'create' collection":
    checkOk sdb.createCollection("smanual")
    check "smanual" in sdb.keys.toSeq

  test "Command: 'listCollections'":
    check "smanual" in sdb.keys.toSeq

  test "Command: 'renameCollection'":
    check sco.insert(%*{})
    check sco.rename "syncnew"
    check sco.rename "sync"

suite "Mongo connection error-handling operations":
  test "Command: 'getLastError'":
    checkOk sm.getLastError()

  test "Write operations error handling":
    discard sdb.createCollection("smanual")
    let sReplyCreate = sdb.createCollection("smanual")
    check not sReplyCreate.ok
    check sReplyCreate.err.contains("already exists")

suite "Authentication":
  test "Command: 'authenticate', method: 'SCRAM-SHA-1'":
    check(sdb.createUser("test1", "test"))
    let authtest = newMongoWithURI("mongodb://test1:test@localhost:27017/testdb")
    check(authtest.authDb == TestDB)
    checkOk authtest[TestDB][TestCol].insert(%*{"data": "auth"})
    check(authtest.authenticated == true)
    check(sdb.dropUser("test1"))

suite "User Management":
  test "Command: 'createUser' without roles and custom data":
    check(sdb.createUser("testuser", "testpass"))
    check(sdb.dropUser("testuser"))

  test "Command: 'dropUser'":
    check(sdb.createUser("testuser2", "testpass2"))
    check(sdb.dropUser("testuser2"))

suite "Mongo collection-level operations":
  setup:
    discard drop sco

  test "'count' documents in collection":
    checkOk:
      sco.insert:
        @[
          %*{"iter": 0.int32, "label": "l"},
          %*{"iter": 1.int32, "label": "l"},
          %*{"iter": 2.int32, "label": "l"},
          %*{"iter": 3.int32, "label": "l"},
          %*{"iter": 4.int32, "label": "l"},
        ]
    check(sco.len == 5)

  test "'drop' collection":
    check(sco.insert(%*{"svalue": "hello"}))
    discard drop sco
    check(sco.find(%*{"svalue": "hello"}).items.toSeq.len == 0)

suite "Mongo client operations test suite":
  setup:
    discard drop sco

  test "Mongo object `$` operator":
    check($sm == "mongodb://127.0.0.1:27017")

  test "Taking database":
    check($sdb == "testdb")

  test "Taking collection":
    check($sco == "testdb.sync")

  test "Inserting single document":
    check(sco.insert(%*{"double": 3.1415}))

    check(sco.find(%*{"double": 3.1415}).items.toSeq.len == 1)

  test "Inserting multiple documents":
    let
      doc1 = %*{"integer": 100'i32}
      doc2 = %*{"string": "hello", "subdoc": {"name": "John"}}
      doc3 = %*{"array": ["element1", "element2", "element3"]}
    check sco.insert(@[doc1, doc2, doc3])

  test "Update single document":
    let
      selector = %*{"integer": "integer"}
      updater  = %*{"$set": {"integer": "string"}}
    check sco.insert(@[selector, selector])
    check sco.update(selector, updater, false, false)
    check sco.find(%*{"integer": "string"}).items.toSeq.len == 1

  test "Update multiple documents":
    let
      selector = %*{"integer": 100'i32}
      doc1 {.used.} = %*{"integer": 100'i32}
      doc2 {.used.} = %*{"integer": 100'i32}
      doc3 {.used.} = %*{"integer": 100'i32}
      doc4 {.used.} = %*{"integer": 100'i32}
      updater  = %*{"$set": {"integer": 200'i32}}
    check sco.insert(@[doc1, doc2])
    check sco.update(selector, updater, true, false)
    check sco.find(%*{"integer": 200'i32}).items.toSeq.len == 2

  test "Upsert":
    let
      selector = %*{"integer": 100'i64}
      updater  = %*{"$set": {"integer": 200'i64}}
    check sco.update(selector, updater, false, true)
    check sco.find(%*{"integer": 200}).items.toSeq.len == 1

  test "Remove single document":
    let doc = %*{"string": "hello"}
    check sco.insert(doc)
    checkOk sco.remove(doc, limit=1)

  test "Remove multiple documents":
    check sco.insert(@[%*{"string": "value"}, %*{"string": "value"}])
    checkOk sco.remove(%*{"string": "value"})
    check sco.find(%*{"string": "value"}).items.toSeq.len == 0

suite "Mongo aggregation commands":
  setup:
    discard drop sco

  test "Count documents in query result":
    checkOk sco.insert(@[%*{"string": "value"}, %*{"string": "value"}])
    check(sco.find(%*{"string": "value"}).len == 2)

  test "Query distinct values by field in collection documents":
    checkOk sco.insert(@[%*{"string": "value", "int": 1'i64}, %*{"string": "value", "double": 2.0}])
    check(sco.find(%*{"string": "value"}).unique("string") == @["value"])

  test "Sort query results":
    checkOk sco.insert(@[%*{"i": 5}, %*{"i": 3}, %*{"i": 4}, %*{"i": 2}])
    let res = sco.find(%*{}).orderBy(%*{"i": 1}).items.toSeq
    check "conversions":
      res[0]["i"].toInt == 2
      res[^1]["i"].toInt == 5

suite "Mongo client querying test suite":
  setup:
    discard drop sco

  test "Query single document":
    let myId = genOid()
    check(sco.insert(%*{"string": "somedoc", "myid": myId}))
    check(sco.find(%*{"myid": myId}).first["myid"].toOid() == myId)

  test "Query multiple documents as a sequence":
    check(sco.insert(@[%*{"string": "value"}, %*{"string": "value"}]))
    check(sco.find(%*{"string": "value"}).items.toSeq.len == 2)

  test "Query multiple documents as iterator":
    check(sco.insert(%*{"string": "hello"}))
    check(sco.insert(%*{"string": "hello"}))
    for document in sco.find(%*{"string": "hello"}).items():
      check(document["string"].toString == "hello")

  test "Query multiple documents up to limit":
    check(sco.insert(
      @[
        %*{"iter": 0.int32, "label": "l"},
        %*{"iter": 1.int32, "label": "l"},
        %*{"iter": 2.int32, "label": "l"},
        %*{"iter": 3.int32, "label": "l"},
        %*{"iter": 4.int32, "label": "l"}
      ]
    ))
    check(sco.find(%*{"label": "l"}).limit(3).items.toSeq.len == 3)

  test "Skip documents":
    check(sco.insert(
      @[
        %*{"iter": 0.int32, "label": "l"},
        %*{"iter": 1.int32, "label": "l"},
        %*{"iter": 2.int32, "label": "l"},
        %*{"iter": 3.int32, "label": "l"},
        %*{"iter": 4.int32, "label": "l"},
      ]
    ))
    check(sco.find(%*{"label": "l"}).skip(3).items.toSeq.len == 2)

when compileOption"threads":
  suite "Mongo tailable cursor operations (with threads)":
    setup:
      discard drop sco
      discard drop sdb["capped"]

    test "Read documents from capped collection":
      checkOk sdb.createCollection("capped", capped=true, maxSize=10000)
      let sccoll = sdb["capped"]
      checkOk sccoll.insert(%*{"iter": 0.int32, "label": "t"})

      proc inserterSync(sccoll: Collection[Mongo]) {.thread.} =
        sleep(1000)
        checkOk sccoll.insert(%*{"iter": 1.int32, "label": "t"})
        checkOk sccoll.insert(%*{"iter": 2.int32, "label": "t"})
        sleep(500)
        checkOk sccoll.insert(%*{"iter": 3.int32, "label": "t"})

      proc readerSync(sccoll: Collection[Mongo]) {.thread.} =
        let cur = sccoll.find(%*{"label": "t"}, maxTime=5000).tailableCursor().awaitData()
        var counter = 0
        while counter < 4:
          let data = cur.next()
          if data.len > 0:
            check(data[0]["iter"].toInt32 < 4.int32)
            counter += 1
        let data = cur.next()
        check(data.len == 0)

      var thr: array[2, Thread[Collection[Mongo]]]
      createThread[Collection[Mongo]](thr[1], readerSync, sccoll)
      createThread[Collection[Mongo]](thr[0], inserterSync, sccoll)
      joinThreads(thr)
      discard drop sccoll

else:
  suite "Mongo tailable cursor operations (no threads)":
    setup:
      discard drop sco
      discard drop sdb["capped"]

    test "Read documents one by one in collection":
      checkOk sdb.createCollection("capped", capped=true, maxSize=10000)
      let sccoll = sdb["capped"]
      var found = sccoll.find(%*{"label": "t"}, maxTime=1500)
      let cur = awaitData: tailableCursor found
      checkOk sccoll.insert(%*{"iter": 0.int32, "label": "t"})
      var data: seq[Bson] = @[]
      data = cur.next()
      check(data.len == 1)
      check(data[0]["iter"].toInt32 == 0.int32)
      checkOk sccoll.insert(%*{"iter": 1.int32, "label": "t"})
      data = cur.next()
      check(data.len == 1)
      check(data[0]["iter"].toInt32 == 1.int32)
      checkOk sccoll.insert(%*{"iter": 2.int32, "label": "t"})
      data = cur.next()
      check(data.len == 1)
      check(data[0]["iter"].toInt32 == 2.int32)
      checkOk sccoll.insert(%*{"iter": 3.int32, "label": "t"})
      data = cur.next()
      check(data.len == 1)
      check(data[0]["iter"].toInt32 == 3.int32)
      data = cur.next()
      check(data.len == 0)
      checkOk sccoll.drop()

  block:
    ## clean up testing garbage
    discard drop sco
