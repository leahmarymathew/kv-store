# Contributing

## Prerequisites

- Go 1.21 or later (`go version`)
- No external dependencies beyond the standard library (check `go.mod`)

## Running Tests

```sh
# All packages, with race detector
go test ./... -race -count=1

# Single package
go test ./internal/store/... -v

# Integration tests only (starts real TCP servers)
go test ./tests/... -v -timeout 120s

# Replication tests (includes a 6-second reconnect test)
go test ./internal/replication/... -v -timeout 60s
```

The `-count=1` flag disables Go's test result cache so tests always re-run.

## Project Structure

```
cmd/            Binary entry points (server, client, bench)
internal/       All library code — nothing in here imports cmd/
  protocol/     Wire format encode/decode
  store/        In-memory store, TTL heap, WAL-backed wrapper
  wal/          Append-only log and crash recovery
  server/       TCP server and per-connection handler
  replication/  Primary, replica, replication buffer
  cluster/      Consistent hash ring and key router
bench/          Python chart generator (analyze.py)
docs/           Protocol spec, architecture, benchmark results
scripts/        PowerShell cluster start/stop helpers
tests/          Integration and crash-recovery tests
```

## Making a Change

1. Write or update a test first.
2. Run `go test ./... -race` — all tests must pass.
3. Run `go vet ./...` — no warnings.
4. Keep each commit focused: one logical change per commit.

## Key Interfaces

- `server.StoreBackend` — implemented by `*store.WALStore` (standalone) and `*server.PrimaryBackend` (cluster primary). Any new backend must satisfy this interface.
- `replication.Primary` / `replication.Replica` — the replication wire format is described in [`docs/architecture.md`](../docs/architecture.md). Changes to it require updating both sides and bumping the handshake version.

## Benchmark Workflow

```sh
go build -o server.exe ./cmd/server
go build -o bench.exe  ./cmd/bench

.\server.exe &
.\bench.exe -mode throughput -output csv
.\bench.exe -mode latency    -output csv
.\bench.exe -mode stress     -output csv
python bench/analyze.py          # generates PNG charts
```

Results land in `bench_results/`. Charts land in `bench_results/charts/`. Neither directory is committed to version control.
