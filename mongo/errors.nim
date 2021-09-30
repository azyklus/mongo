type
  CommunicationError* = object of CatchableError
    ## Raises on communication problems with MongoDB server

  MongoError* = object of CatchableError
    ## Base exception for nimongo error (for simplifying error handling)

  NotFound* = object of MongoError
    ## Raises when querying of one documents returns empty result

  ReplyFieldMissing* = object of MongoError
    ## Raises when reqired field in reply is missing

  OperationTimeout* = object of MongoError
    ## Raises when operation required error occurs

template raiseIfMissingSsl*(): untyped =
  ## raise a "missing ssl" error when ssl is missing
  when not defined(ssl):
    raise MongoError.newException "use --define:ssl to enable ssl/tls"
