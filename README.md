# Distributed In-Memory Key-Value Store

A distributed key-value store written in Go from scratch — custom binary TCP protocol, write-ahead log, consistent hashing, and primary-replica replication — achieving **17,600+ ops/sec** at **p99 < 30ms** under 200 concurrent clients on a quad-core i5.

## Features

- **Custom binary TCP protocol** — not RESP; compact 9-byte fixed header per request
- **Goroutine-per-connection** with semaphore-based limiting (max 1,000 concurrent)
- **TTL expiry** — min-heap active expiry scan + lazy expiry on read, O(log n) insert
- **Write-ahead log** — CRC32-checksummed entries, automatic crash recovery on startup
- **Consistent hashing** — 150 virtual nodes per real node for uniform key distribution
- **Primary-replica replication** — async log streaming over TCP with full-resync on reconnect
- **Cluster routing** — `Router` maps keys to node addresses via the hash ring
- **Benchmark suite** — throughput / latency / stress / pipeline modes with CSV + chart output

## Architecture

```
Client Connections (up to 1,000 concurrent)
        │
TCP Server (goroutine-per-connection + semaphore)
        │
Protocol Parser (custom binary: cmd│keyLen│key│valLen│val)
        │
StoreBackend (interface: standalone WALStore or PrimaryBackend)
        │
WALStore (write-ahead log wrapping Store)
       ╱ ╲
    WAL    Store (sync.RWMutex + TTL min-heap)
  (disk)  (memory: map[string][]byte)
        │
Replication Buffer (circular, 10,000 entries)
       ╱ ╲
 Replica1  Replica2
 (async TCP stream — 10ms poll interval)

Consistent Hash Ring (150 virtual nodes)
(routes client keys → node addresses in cluster mode)
```

## Wire Protocol

**Request**
```
┌─────────┬────────────┬──────────┬────────────┬──────────┐
│  1 byte │   4 bytes  │  N bytes │   4 bytes  │  M bytes │
│   cmd   │  key len   │   key    │  value len │  value   │
└─────────┴────────────┴──────────┴────────────┴──────────┘
```

**Response**
```
┌─────────┬────────────┬──────────┐
│  1 byte │   4 bytes  │  N bytes │
│ status  │ payload len│ payload  │
└─────────┴────────────┴──────────┘
```

All multi-byte integers are big-endian. See [`docs/protocol.md`](docs/protocol.md) for command and status code tables.

## Getting Started

### Prerequisites
- Go 1.21+

### Build

```sh
go build ./...
```

### Run Standalone

```sh
.\server.exe -port 7379 -wal-path wal.log
```

### Connect with the CLI client

```
.\client.exe
> set foo bar
> get foo
> ttl foo 30
> delete foo
> ping
> quit
```

### Run Cluster (Primary + 2 Replicas)

```powershell
.\scripts\start_cluster.ps1
# Primary:   localhost:7379
# Replica 1: localhost:7381
# Replica 2: localhost:7382

.\scripts\stop_cluster.ps1
```

### Run Benchmarks

```sh
# Four modes — server must be running first
.\bench.exe -mode throughput -output csv
.\bench.exe -mode latency    -output csv
.\bench.exe -mode stress     -output csv
.\bench.exe -mode pipeline   -output csv

# Generate charts (requires pandas + matplotlib)
python bench/analyze.py
```

### Run Tests

```sh
go test ./... -race -count=1
```

## Benchmark Results

Measured on Intel Core i5-1035G1 @ 1.00GHz, loopback, 64-byte payloads, mixed SET/GET:

| Clients | Ops/sec | p50 | p95 | p99 |
|---------|---------|-----|-----|-----|
| 50 | 9,054 | 4.53ms | 11.13ms | 20.39ms |
| 100 | 11,790 | 6.89ms | 17.24ms | 29.68ms |
| 200 | 17,636 | 10.51ms | 17.09ms | 28.20ms |
| 500 | run benchmark | — | — | — |

Full methodology and latency histograms: [`docs/benchmark_results.md`](docs/benchmark_results.md)

## Project Structure

```
kv-store/
├── cmd/
│   ├── server/main.go          entry point — flags, WALStore, Server, cluster modes
│   ├── client/main.go          interactive CLI client (REPL)
│   └── bench/main.go           benchmark runner (throughput/latency/stress/pipeline)
├── internal/
│   ├── protocol/
│   │   ├── parser.go           decode raw bytes → Command struct
│   │   └── serializer.go       encode Response → bytes
│   ├── store/
│   │   ├── store.go            in-memory map with sync.RWMutex
│   │   ├── ttl.go              min-heap TTL expiry
│   │   └── wal_store.go        WAL-backed Store wrapper (StoreBackend)
│   ├── wal/
│   │   ├── wal.go              append-only log, CRC32 per entry
│   │   └── recovery.go         replay WAL into Store on startup
│   ├── server/
│   │   ├── server.go           TCP listener, semaphore, graceful shutdown
│   │   ├── connection.go       per-connection read/dispatch/write loop
│   │   └── primary_backend.go  StoreBackend that routes writes through Primary
│   ├── replication/
│   │   ├── replication_log.go  circular ReplicationBuffer (10k entries)
│   │   ├── primary.go          TCP listener, streams entries to replicas
│   │   └── replica.go          connects to primary, applies entries, reconnects
│   └── cluster/
│       ├── consistent_hash.go  HashRing — virtual nodes, CRC32, sort.Search
│       └── router.go           Router — maps NodeID → NodeAddress
├── bench/
│   └── analyze.py              pandas/matplotlib chart generator
├── docs/
│   ├── architecture.md         detailed component and protocol documentation
│   ├── benchmark_results.md    full benchmark tables and resume bullets
│   └── protocol.md             wire protocol byte-level spec
├── scripts/
│   ├── start_cluster.ps1       build + launch primary + 2 replicas
│   └── stop_cluster.ps1        kill all server.exe processes
├── tests/
│   ├── crash_recovery_test.go  WAL crash and partial-write recovery tests
│   └── integration_test.go     full cluster integration tests
├── .github/
│   └── CONTRIBUTING.md         how to contribute and run tests
├── go.mod
└── go.sum
```

## Design Decisions

### 1. Goroutine-per-connection instead of epoll

Go's M:N scheduler multiplexes goroutines across OS threads with sub-microsecond context switches, making one goroutine per connection practical at thousands of connections. A semaphore (`chan struct{}` of size 1,000) caps concurrent connections so memory stays bounded — each idle goroutine costs ~4 KB of stack versus the complexity of manual I/O multiplexing with epoll.

### 2. Min-heap for TTL expiry

A min-heap keeps the soonest-expiring key at the top, so the background expiry loop only wakes when a key is actually due. This avoids an O(n) full-scan every tick — the heap gives O(log n) insert and O(log n) deletion, with O(1) peek at the next expiry time. Lazy expiry on every GET ensures keys are never returned after their deadline even if the background loop hasn't fired yet.

### 3. Write-ahead ordering (WAL before store update)

The WAL entry is flushed to disk with `fsync` before the in-memory store is updated. If the process crashes between the two, recovery replays the WAL and the write is applied — the store converges to the same state. Without write-ahead ordering a crash between store update and WAL append would leave the log inconsistent with memory.

### 4. CRC32 instead of MD5/SHA

CRC32 is implemented in hardware on all modern x86 CPUs (SSE4.2 `crc32` instruction), making it effectively free compared to a hash computation. For an append-only log the threat model is bit-flip corruption, not adversarial tampering, so collision resistance is irrelevant — CRC32's error-detection properties are sufficient and add zero measurable latency per entry.

### 5. Asynchronous replication

The primary does not wait for replica acknowledgement before returning OK to the client. This keeps write latency low and prevents a slow or disconnected replica from stalling the primary. The tradeoff is that a primary crash between a successful write and replica delivery loses that write on replicas — acceptable for a cache tier but would require synchronous quorum writes (Raft) for strong durability guarantees.

## What I Would Add Next

- **Raft consensus** for automatic primary election on failure (no manual failover)
- **WAL snapshotting** to compact the log after N entries (currently unbounded growth)
- **gRPC** instead of the custom binary protocol for better observability and client generation
- **Prometheus `/metrics` endpoint** for latency histograms and replication lag gauges
- **Client-side connection pooling** in the load generator to eliminate per-request dial cost
