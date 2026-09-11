# xmip-core-transport-aws-kinesis

Amazon Kinesis Data Streams transport: Signature Version 4 over the JSON API — put a Stream as one record, read a shard on from where it last stopped — a stream and its shard are a Location. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
