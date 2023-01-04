# mongo

[![Test Matrix](https://github.com/disruptek/mongo/workflows/CI/badge.svg)](https://github.com/disruptek/mongo/actions?query=workflow%3ACI)
[![GitHub release (latest by date)](https://img.shields.io/github/v/release/disruptek/mongo?style=flat)](https://github.com/disruptek/mongo/releases/latest)
![Minimum supported Nim version](https://img.shields.io/badge/nim-1.9.1%2B-informational?style=flat&logo=nim)
[![License](https://img.shields.io/github/license/disruptek/mongo?style=flat)](#license)

It's a fork of `nimongo` for production use.

## Usage Notes

- [x] define:ssl
- [x] mm:arc

Set the `DNS_SERVER` environmental variable to force a specific DNS server
for the purposes of replica resolution.

When connecting to Atlas or other clusters, we'll open `maxConnections` per
each replica.

## License
MIT
