# Architecture

## 1. Component Overview

### TCP Server (`internal/server`)
The server opens a `net.Listener` and accepts connections in a loop. Each accepted connection immediately acquires a slot from a buffered channel semaphore of size 1,000; if the channel is full the accept loop blocks until a slot is freed, applying backpressure rather than rejecting connections. Each connection runs in its own goroutine, which reads commands, dispatches them to the `StoreBackend`, and writes responses using a `bufio.Writer` for batched TCP sends. A `context.Context` drives graceful shutdown: the listener is closed, new connections stop, and the server waits for all in-flight goroutines to drain before returning.

### Protocol Layer (`internal/protocol`)
The binary protocol uses a fixed 9-byte request header: 1 byte command, 4 bytes key length (big-endian), 4 bytes value length (big-endian), followed by key bytes then value bytes. Responses are a 1-byte status code followed by a 4-byte payload length and payload bytes. The parser reads directly from a `bufio.Reader` to avoid per-read syscalls. No framing delimiter is needed because the length fields are always present — the reader knows exactly how many bytes to consume.

### Store (`internal/store`)
The core store is a `map[string][]byte` guarded by a `sync.RWMutex`. GET acquires a read lock; SET and DELETE acquire a write lock. A separate min-heap (`container/heap`) tracks TTL expiry times. On every GET the key's deadline is checked before returning the value (lazy expiry). A background goroutine ticks at a configurable interval, pops the heap, and deletes all entries whose deadline has passed (active expiry). The `WALStore` wraps the core store and sequences every write through the WAL before it touches memory.

### Write-Ahead Log (`internal/wal`)
The WAL is an append-only binary file. Each entry contains a sequence number, entry type byte, key length, key bytes, value length, value bytes, TTL seconds, and a CRC32 checksum over all preceding fields. On startup, `recovery.go` reads entries sequentially, verifies each checksum, and replays them into an empty store. If a checksum fails the tail is truncated at that point — any subsequent entries are discarded. This guarantees that a partial write (power loss mid-append) does not corrupt previously committed data.

### Replication Buffer (`internal/replication`)
The `ReplicationBuffer` is a fixed-capacity circular slice of `ReplicationEntry` structs. Each entry stores a monotonically increasing sequence number assigned at append time. When the buffer is full the oldest entry is silently overwritten (head advances). `GetSince(afterSeq)` walks from head to tail and returns all entries with a sequence number greater than `afterSeq`. If `afterSeq` is older than the oldest surviving entry, `needsResync=true` is returned, triggering a full key snapshot from the primary.

### Primary (`internal/replication`)
The `Primary` wraps a `WALStore` with `WrapSet`, `WrapDelete`, and `WrapSetWithTTL` methods that write to both the store and the replication buffer atomically. It listens on a separate TCP port for replica connections. Each replica connection is handled by `handleReplica`, which performs the handshake, sends any buffered catch-up entries (or a full resync snapshot), then polls the buffer every 10ms and streams new entries over the connection.

### Replica (`internal/replication`)
The `Replica` dials the primary's replication port, sends a handshake containing its last seen sequence number, and then enters `receiveLoop`. Entries are applied to the replica's local `WALStore` in the order received. On disconnect the replica sleeps 5 seconds and reconnects, sending its last known sequence number so the primary can resume streaming from that point.

### Consistent Hash Ring (`internal/cluster`)
The `HashRing` stores virtual nodes — each real node gets `replicas` (default 150) virtual entries on the ring, keyed by `"nodeID#i"` hashed with CRC32. The ring slice is kept sorted by hash value. `GetNode(key)` hashes the key and uses `sort.Search` to find the first virtual node at or beyond that hash, wrapping to index 0 if the hash exceeds all virtual node positions. The `Router` maps node IDs to `NodeAddress` structs (host + port) and exposes `RouteKey` for single-node routing and `RouteKeyToN` for replication-factor routing.

---

## 2. Request Lifecycle

The following steps trace a `SET foo bar` from client to acknowledgement:

1. **Client** writes `[0x02][0x00 0x00 0x00 0x03][foo][0x00 0x00 0x00 0x03][bar]` to the TCP connection.
2. **Server accept loop** receives the connection. Semaphore channel has a free slot — goroutine starts.
3. **`connection.go` read loop** calls `parser.Parse(bufReader)`:
   - reads 1 byte → cmd = SET (0x02)
   - reads 4 bytes → key length = 3
   - reads 3 bytes → key = "foo"
   - reads 4 bytes → value length = 3
   - reads 3 bytes → value = "bar"
4. **Dispatch** calls `backend.Set("foo", []byte("bar"))`.
5. **`WALStore.Set`** (or `PrimaryBackend.WrapSet`):
   a. Serializes a WAL entry: `[seq][type=SET][keyLen][key][valLen][val][ttl=0][crc32]`
   b. Appends to the WAL file and calls `fsync`.
   c. Acquires write lock on the in-memory store.
   d. Inserts `map["foo"] = []byte("bar")`.
   e. Releases write lock.
6. **Primary (cluster mode only)**: appends the same entry to the `ReplicationBuffer`.
7. **Dispatch** returns `nil` error → response is `StatusOK` with empty payload.
8. **`connection.go`** writes `[0x00][0x00 0x00 0x00 0x00]` to the `bufio.Writer`, then flushes.
9. **Client** reads the 5-byte response and displays `OK`.

---

## 3. WAL Entry Format

Each entry in `wal.log` has the following layout:

```
┌────────────┬──────────┬──────────┬───────────┬──────────┬────────────┬───────────┬──────────┬─────────┐
│  8 bytes   │  1 byte  │  4 bytes │  N bytes  │  4 bytes │  M bytes   │  8 bytes  │  4 bytes │
│  sequence  │   type   │  keyLen  │    key    │  valLen  │   value    │  ttlSecs  │  crc32   │
└────────────┴──────────┴──────────┴───────────┴──────────┴────────────┴───────────┴──────────┘
```

- **sequence**: monotonically increasing uint64, used for recovery ordering and replication catch-up
- **type**: `0x01` SET, `0x02` DELETE, `0x03` TTL
- **ttlSecs**: seconds until expiry; `0` means no TTL
- **crc32**: IEEE CRC32 checksum over all preceding bytes in this entry; used to detect truncated or corrupted tail entries

On recovery, each entry is read, its checksum verified, and applied to the store in sequence order. The first checksum failure truncates the log at that position.

---

## 4. Consistent Hash Ring

### Virtual Nodes

With three real nodes (`A`, `B`, `C`) and `replicas=3` (reduced for clarity), the ring contains 9 virtual nodes:

```
Hash space: 0 ──────────────────────────────────── 2³²-1

Virtual nodes (sorted by CRC32 hash of "nodeID#i"):
  pos 0x1A3C  → A#0
  pos 0x2F88  → C#1
  pos 0x41B2  → B#0
  pos 0x6E04  → A#2
  pos 0x8AC1  → C#0
  pos 0xA319  → B#2
  pos 0xC507  → A#1
  pos 0xE012  → C#2
  pos 0xF4AA  → B#1
```

### Worked Example

Key `"user:42"` hashes to CRC32 `0x7900` (example).

`sort.Search` finds the first virtual node with hash ≥ `0x7900`:
→ `0x8AC1` → owner `C#0` → real node **C**.

Key `"session:99"` hashes to `0xF900` (beyond all virtual nodes):
→ index wraps to 0 → `0x1A3C` → **A#0** → real node **A**.

With 150 virtual nodes per real node the ring has 450 entries. Statistical analysis of 10,000 sequential keys shows each node receives 29–37% of keys — a maximum deviation of ~4% from the ideal 33.3% — without any rebalancing.

### Consistent Hashing Property

When a fourth node **D** is added, only keys that fall in the arc between D's new virtual positions and the virtual nodes that previously owned those positions move. In testing, adding a 4th node moved 19.4% of 1,000 keys — well under the theoretical maximum of 25% (1 / (n+1)).

---

## 5. Replication Protocol

### Handshake Sequence

```
Replica                                Primary
   │                                      │
   │──── TCP connect ──────────────────►  │
   │                                      │
   │  [8 bytes: lastSeq uint64 BE]        │
   │  [4 bytes: idLen uint32 BE]          │
   │──── handshake ────────────────────►  │
   │  [idLen bytes: replicaID]            │
   │                                      │
   │  if lastSeq too old OR store has     │
   │  data not in buffer:                 │
   │                                      │
   │  ◄── [0xFF marker] (FULL_SYNC) ───   │
   │  ◄── [SET entry for every key] ───   │
   │                                      │
   │  else:                               │
   │  ◄── [entries since lastSeq] ──────  │
   │                                      │
   │  ◄── stream (poll every 10ms) ─────  │
   │  ◄── new entries as they arrive ───  │
   │                                      │
```

### Entry Wire Format (stream)

```
┌─────────┬──────────┬──────────┬───────────┬──────────┬───────────┬──────────┐
│  1 byte │  8 bytes │  4 bytes │  N bytes  │  4 bytes │  M bytes  │  8 bytes │
│  type   │   seq    │  keyLen  │    key    │  valLen  │   value   │  ttlSecs │
└─────────┴──────────┴──────────┴───────────┴──────────┴───────────┴──────────┘
```

Type `0xFF` is the FULL_SYNC marker — it uses the same wire format with empty key/value/ttl fields, signalling the replica to clear its local store before applying subsequent SET entries.

### Sequence Numbers and Reconnect

Every entry in the `ReplicationBuffer` carries a sequence number assigned at append time. The replica tracks `lastSeq` and sends it in every handshake. On reconnect the primary calls `GetSince(lastSeq)`:
- If entries are still in the buffer → stream the delta (no data loss)
- If the buffer has wrapped past `lastSeq` → `needsResync=true` → full snapshot

The circular buffer holds 10,000 entries. At typical write rates this covers several minutes of writes, making full resyncs rare in practice.

---

## 6. Failure Modes

| Failure | Detected How | Recovery Mechanism | Data Loss? |
|---------|-------------|-------------------|------------|
| **Server crash during WAL append** (power loss mid-write) | CRC32 checksum mismatch on the partial tail entry at next startup | WAL truncated at first bad checksum; all fully-committed entries before it are replayed | Last in-flight write only (the partial entry) |
| **Crash after WAL fsync but before store update** | WAL has the entry; in-memory store does not | Recovery replays the WAL entry into the store on startup; converges to correct state | None |
| **Replica network partition** | Primary's `conn.Write` returns error; replica's `io.ReadFull` returns EOF | Primary removes replica from map; replica sleeps 5s then reconnects with its `lastSeq`; primary streams the delta or sends full resync | None if buffer hasn't wrapped; otherwise replica receives a full snapshot |
| **Corrupt WAL tail entry** (bit flip on disk) | CRC32 mismatch on the corrupt entry | Truncate log at that entry; replay everything before it; log a warning with the truncation sequence number | Entries after the corrupt byte — typically just the last append |
