description = "Pure Nim driver for MongoDB"
version     = "0.7.0"
license     = "MIT"
author      = "Rostyslav Dzinko <rostislav.dzinko@gmail.com>; all bugs provided by disruptek"

requires "https://github.com/ba0f3/scram.nim#head"
requires "https://github.com/azyklus/adix#head"
requires "https://github.com/disruptek/assume"
requires "https://github.com/ba0f3/dnsclient.nim >= 0.1.1 & < 1.0.0"

when not defined(release):
  requires "https://github.com/disruptek/balls >= 3.6.0"
