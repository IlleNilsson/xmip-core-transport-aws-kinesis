# xmip-core-transport-aws-kinesis

Amazon Kinesis Data Streams transport: Signature Version 4 over the JSON API — put a Stream as one record, read a shard on from where it last stopped — a stream and its shard are a Location. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

Signature Version 4 and the JSON 1.1 protocol come from [xmip-core-transport-aws](https://github.com/IlleNilsson/xmip-core-transport-aws), where every AWS technology shares what AWS speaks over HTTP (ADR-0044, amendment 2026-09-24); HTTP itself comes from [xmip-core-transport-http](https://github.com/IlleNilsson/xmip-core-transport-http).

A Receive Location reads on with the `NextShardIterator` the last read handed back, and asks `GetShardIterator` for a new one only on its first receive or where the kept one is refused — expired after five minutes unused, or its shard gone. Until 2026-09-28 every receive asked for a new iterator.

## How a received record is acknowledged

A shard is read, never consumed, so what an acknowledgement moves is the transport's own place: the sequence number of the last record the runtime accepted after its whole receive cycle (runtime-model section 5). Each record is handed on whole. Accepted moves the place to that record only where it stands at the record before it (`transport::contiguous::Contiguous`), and the last record's acceptance keeps the read's `NextShardIterator`; Refused moves the place the same way: a shard has no place to reject a record into, and a refused record is not read again (the runtime audited the refusal). Failed moves nothing and drops the iterator, so the next receive asks `GetShardIterator` for the records after the place and reads the failed record and every one after it again — at-least-once, never a skip. Acceptance and refusal cost no request; a failed cycle costs one `GetShardIterator` on the next receive.

Requests go on connections kept between them (`http::endpoint::Connections`, offering HTTP/1.1): the transport holds them and hands them to every client it makes, so a call costs one exchange and not a connect, a TLS handshake and a `Connection: close`, as it did until 2026-09-27.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
