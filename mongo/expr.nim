import std/strutils
import std/macros

import pkg/assume

import mongo/bson

type
  Operator = enum
    eq = "=="
    ne = "!="
    gt = ">"
    lt = "<"
    lte = "<="
    gte = ">="

    `and` = "and"
    `or` = "or"

    `not` = "not"
    nor = "nor"
    `in` = "in"
    nin = "notin"

    `type` = "is"
    exists = "exists"
    all = "all"
    size = "len"

const
  comparisons = {eq, ne, gt, lt, lte, gte, `in`, nin, `type`, exists, all, size}
  logicals = {`and`, `or`, `not`, nor}

proc assignField(doc, field, value: NimNode): NimNode =
  result = value
  if not doc.isNil:
    doc.add:
      field.colon value

proc stringifyIdentifier(n: NimNode): NimNode =
  n.expectKind nnkIdent
  result = newLit n.strVal

proc parseOperator(op: NimNode): Operator =
  parseEnum[Operator](op.strVal)

proc renderOperator(op: Operator): NimNode =
  newLit "$" & enumValueAsString(op)

proc queryOperator(op: NimNode; value: NimNode): NimNode =
  ## compose a query operator against value
  try:
    var operator = parseOperator op
    var rendered = renderOperator operator
    result = nnkTableConstr.newTree(rendered.colon value)
  except ValueError:
    result = op.errorAst "unsupported query operator"

proc unwrapDotExpr(n: NimNode): NimNode =
  # hacking ufcs basically
  if n[0].kind == nnkDotExpr:
    # foo.all [1, 2, 3]
    let kind = n.kind
    result = kind(n).newTree(n[0][1], n[0][0])
    for peer in n[1..^1]:
      result.add peer
  elif n[1].kind == nnkDotExpr:
    # foo.len == 3
    let kind = n.kind
    result = kind(n).newTree(n[1][1], n[1][0])
    for peer in n[2..^1]:
      result.add peer

proc parseExpression(doc, n: NimNode): NimNode =
  case n.kind
  of nnkIdent:
    result = stringifyIdentifier n
  of nnkInfix, nnkCall, nnkCommand:
    if n[0].kind == nnkDotExpr:
      return parseExpression(doc, unwrapDotExpr n)
    if n[1].kind == nnkDotExpr:
      return parseExpression(doc, unwrapDotExpr n)

    let op = parseOperator n[0]
    case op
    of comparisons:
      result =
        doc.assignField stringifyIdentifier(n[1]):
          queryOperator(n[0], n[2])
    of logicals:
      var rendered = renderOperator op
      let a = nnkTableConstr.newNimNode(n)
      discard parseExpression(a, n[1])
      let b = nnkTableConstr.newNimNode(n)
      discard parseExpression(b, n[2])
      result =
        doc.assignField rendered:
          nnkBracket.newTree(a, b)
  of nnkPar:
    # use the parser to figure this one out
    if n.len == 1:
      result = parseExpression(doc, n[0])
    else:
      error("we don't handle tuples yet", n)
      result = newEmptyNode()
  else:
    error("unsupported ast " & $n.kind, n)
    result = newEmptyNode()

proc parseStatement(doc: NimNode; n: NimNode): NimNode =
  case n.kind
  of nnkCallKinds:
    parseExpression(doc, n)
  else:
    n.errorAst "unsupported"

macro expr*(n: untyped): untyped =
  n.expectKind nnkStmtList
  var doc = nnkTableConstr.newNimNode(n)
  for stmt in n.items:
    discard parseStatement(doc, stmt)
  result = newCall(bindSym"%*", doc)
