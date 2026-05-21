# kv-store

A TCP key-value store written in Go, built from scratch with a custom binary protocol, TTL expiry, and a write-ahead log for crash recovery.

## Features

- Custom binary wire protocol over TCP
- Commands: `GET`, `SET`, `DELETE`, `TTL`, `PING`
- Key expiry via TTL with min-heap scheduling
- Write-ahead log (WAL) with CRC32 checksums and crash recovery on startup
- Connection limiting, per-connection read/write deadlines, buffered I/O
- Graceful shutdown on `Ctrl+C`
- Interactive CLI client

## Quick start

```sh
# Build
go build -o server.exe ./cmd/server
go build -o client.exe ./cmd/client

# Start the server (default port 7379)
./server.exe

# Connect with the CLI client
./client.exe
```

### Server flags

| Flag | Default | Description |
|------|---------|-------------|
| `-host` | `0.0.0.0` | Address to listen on |
| `-port` | `7379` | Port to listen on |
| `-wal-path` | `wal.log` | Path to the WAL file |
| `-expiry-interval` | `100ms` | How often to scan for expired keys |

### Client commands

```
> set foo bar
> get foo
> ttl foo 30        # expire key in 30 seconds
> delete foo
> ping
> quit
```

## Wire protocol

Requests: `[1 byte cmd][4 bytes key length BE][key bytes][4 bytes value length BE][value bytes]`

Responses: `[1 byte status][4 bytes payload length BE][payload bytes]`

See [`docs/protocol.md`](docs/protocol.md) for the full command and status code table.

## Project layout

```
cmd/
  server/     server entry point
  client/     interactive CLI client
  bench/      load generator
internal/
  protocol/   binary encoder/decoder
  store/      in-memory store, TTL heap, WAL-backed store
  server/     TCP server, connection handling
  wal/        write-ahead log, recovery
tests/        crash recovery integration tests
docs/         protocol spec, benchmark results
```

## Benchmarks

Measured on a quad-core i5-1035G1 @ 1.00 GHz, loopback, 64-byte payloads:

| Clients | Throughput | p50 | p99 |
|---------|-----------|-----|-----|
| 1 | 2,873 ops/sec | 0.53 ms | 1.70 ms |
| 10 | 6,514 ops/sec | 1.17 ms | 5.85 ms |
| 100 | 11,790 ops/sec | 6.89 ms | 29.68 ms |
| 200 | 17,636 ops/sec | 10.51 ms | 28.20 ms |

Full results and methodology: [`docs/benchmark_results.md`](docs/benchmark_results.md)

## Running tests

```sh
go test ./...
```
