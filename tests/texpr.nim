import std/macros

import balls

import mongo/expr
import mongo/bson

suite "testing expression generation":

  block:
    ## basic query operator
    let b =
      expr:
        foo == 3
    let c = %*{
      "foo": {"$eq": 3}
    }
    check $b == $c

  block:
    ## multiple statements
    let b =
      expr:
        foo == 3
        bar != true
    let c = %*{
      "foo": {"$eq": 3},
      "bar": {"$ne": true}
    }
    check $b == $c

  block:
    ## and/or clauses
    let b =
      expr:
        foo == 3 and bar != true
    let c = %*{
      "$and": [
        {"foo": {"$eq": 3}},
        {"bar": {"$ne": true}},
      ],
    }
    check $b == $c

  block:
    ## nested and/or with parens for good measure
    let b =
      expr:
        foo == 3
        bif > 1 and (bar != true or fiz == "buzz")
    let c = %*{
      "foo": {"$eq": 3},
      "$and": [
        {"bif": {"$gt": 1}},
        {"$or": [
          {"bar": {"$ne": true}},
          {"fiz": {"$eq": "buzz"}},
        ]},
      ],
    }
    check $b == $c

  block:
    ## in and notin; arrays on rhs
    let b =
      expr:
        foo == 3
        bar in ["one", "two"]
        baz notin ["tree", "fort"]
    let c = %*{
      "foo": {"$eq": 3},
      "bar": {"$in", ["one", "two"]},
      "baz": {"$nin", ["tree", "fort"]},
    }
    check $b == $c

  block:
    ## is and type; bson type enums
    let b =
      expr:
        baz is BsonKindNull
        foo is [BsonKindBool, BsonKindOid]
    let c = %*{
      "baz": {"$type": BsonKindNull},
      "foo": {"$type": [BsonKindBool, BsonKindOid]},
    }
    check $b == $c

  block:
    ## size and all; method call syntax
    let b =
      expr:
        foo.len == 3
        bif.all [1, 2, 3]
    let c = %*{
      "foo": {"$size": 3},
      "bif": {"$all": [1, 2, 3]},
    }
    check $b == $c

  block:
    ## duplicate keys
    let b =
      expr:
        foo == 3
        foo > 2
    let c = %*{
      "foo": {"$eq": 3},
      "foo": {"$gt": 2},
    }
    check $b == $c
