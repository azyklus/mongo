description = "Pure Nim driver for MongoDB"
version     = "0.5.1"
license     = "MIT"
author      = "Rostyslav Dzinko <rostislav.dzinko@gmail.com>; all bugs provided by disruptek"

requires "scram >= 0.1.13"
requires "https://github.com/c-blake/adix < 1.0.0"
requires "https://github.com/disruptek/assume >= 0.3.0 & < 1.0.0"
requires "https://github.com/ba0f3/dnsclient.nim >= 0.1.1 & < 1.0.0"

when not defined(release):
  requires "https://github.com/disruptek/balls == 3.5.0"
