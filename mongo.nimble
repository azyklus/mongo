description = "Pure Nim driver for MongoDB"
version     = "0.4.0"
license     = "MIT"
author      = "Rostyslav Dzinko <rostislav.dzinko@gmail.com>; all bugs provided by disruptek"

requires "scram >= 0.1.13"

when not defined(release):
  requires "https://github.com/disruptek/balls > 3.5.0 & < 4.0.0"
