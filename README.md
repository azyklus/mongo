# mongo

It's a fork of `nimongo` for production use.

## Usage Notes

- [x] threads:on
- [x] define:ssl
- [x] gc:arc

Set the `DNS_SERVER` environmental variable to force a specific DNS server
for the purposes of replica resolution.

When connecting to Atlas or other clusters, we'll open `maxConnections` per
each replica.

## License
MIT
