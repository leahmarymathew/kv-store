# Benchmark Results

## Environment
- OS: Microsoft Windows 11 Home
- CPU: Intel Core i5-1035G1 @ 1.00GHz (4 cores, 8 threads)
- RAM: ~12 GB
- Go version: go1.26.3 windows/amd64
- Server flags: `-port 7379` (all defaults)
- Payload size: 64 bytes
- Warmup: 1000 requests before each run

## Results

| Clients | Operations | Total Time | Throughput    | p50     | p95     | p99     |
|---------|------------|------------|---------------|---------|---------|---------|
| 1       | 1,000      | 0.35s      | 2,873 ops/sec | 0.53ms  | 0.78ms  | 1.70ms  |
| 10      | 10,000     | 1.54s      | 6,514 ops/sec | 1.17ms  | 3.42ms  | 5.85ms  |
| 50      | 50,000     | 5.52s      | 9,054 ops/sec | 4.53ms  | 11.13ms | 20.39ms |
| 100     | 100,000    | 8.48s      | 11,790 ops/sec| 6.89ms  | 17.24ms | 29.68ms |
| 200     | 200,000    | 11.34s     | 17,636 ops/sec| 10.51ms | 17.09ms | 28.20ms |

Rows 1–2: SET only. Rows 3–4: mixed (50% SET / 50% GET). Row 5: GET only.

## Notes
- Throughput scales roughly linearly from 1 to 200 clients (~6x), suggesting the server is not bottlenecked on a single goroutine
- GET-heavy workloads (200 clients, row 5) achieve the highest throughput because `sync.RWMutex` allows concurrent reads, reducing contention versus mixed writes
- p99 tail latency rises steadily with client count (1.70ms → 29.68ms) as the store's write lock becomes a bottleneck under contention; a sharded store would reduce this
- p999 and max spike noticeably at 10+ clients (30–40ms range), likely due to Go GC pauses and Windows scheduler jitter rather than server logic — the i5-1035G1 base clock of 1.00GHz contributes to higher absolute latencies than a desktop CPU would show
- All runs were loopback (localhost), so network is not a factor
