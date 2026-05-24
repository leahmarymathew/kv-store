package replication

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/leahmarymathew/kv-store/internal/store"
	"github.com/leahmarymathew/kv-store/internal/wal"
)

// --- helpers ---

func newWALStore(t *testing.T) *store.WALStore {
	t.Helper()
	dir := t.TempDir()
	w, err := wal.NewWAL(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	s := store.NewStore()
	ws := store.NewWALStore(s, w)
	t.Cleanup(func() { ws.Close() })
	return ws
}

func newWALStoreFile(t *testing.T, path string) *store.WALStore {
	t.Helper()
	w, err := wal.NewWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	s := store.NewStore()
	ws := store.NewWALStore(s, w)
	t.Cleanup(func() { ws.Close() })
	return ws
}

// --- buffer tests ---

func TestReplicationBufferAppendAndGet(t *testing.T) {
	buf := NewReplicationBuffer(100)
	for i := 1; i <= 10; i++ {
		buf.Append(EntrySet, []byte(fmt.Sprintf("k%d", i)), []byte("v"), 0)
	}

	all, resync := buf.GetSince(0)
	if resync {
		t.Fatal("unexpected needsResync for seq 0 with fresh buffer")
	}
	if len(all) != 10 {
		t.Fatalf("expected 10 entries, got %d", len(all))
	}

	partial, _ := buf.GetSince(5)
	if len(partial) != 5 {
		t.Fatalf("expected 5 entries after seq 5, got %d", len(partial))
	}
	for _, e := range partial {
		if e.SequenceNum <= 5 {
			t.Fatalf("entry with seq %d should not be included", e.SequenceNum)
		}
	}
}

func TestReplicationBufferCircular(t *testing.T) {
	buf := NewReplicationBuffer(5)
	for i := 1; i <= 8; i++ {
		buf.Append(EntrySet, []byte(fmt.Sprintf("k%d", i)), []byte("v"), 0)
	}

	entries, _ := buf.GetSince(0)
	if len(entries) != 5 {
		t.Fatalf("expected 5 entries (capacity), got %d", len(entries))
	}
	// Oldest surviving entry should be seq 4.
	if entries[0].SequenceNum != 4 {
		t.Fatalf("expected oldest seq 4, got %d", entries[0].SequenceNum)
	}

	// Asking for seq 1 (already overwritten) should trigger needsResync.
	_, needsResync := buf.GetSince(1)
	if !needsResync {
		t.Fatal("expected needsResync=true for overwritten seq")
	}
}

// --- integration tests ---

func startPrimary(t *testing.T, port int) *Primary {
	t.Helper()
	ws := newWALStore(t)
	p := NewPrimary(ws, port)
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Stop() })
	return p
}

func startReplica(t *testing.T, primaryAddr, id string) (*Replica, *store.WALStore) {
	t.Helper()
	ws := newWALStore(t)
	r := NewReplica(ws, primaryAddr, id)
	if err := r.StartSync(); err != nil {
		t.Fatal(err)
	}
	return r, ws
}

func TestPrimaryReplicaSync(t *testing.T) {
	p := startPrimary(t, 7380)
	time.Sleep(20 * time.Millisecond)

	_, rep1 := startReplica(t, "localhost:7380", "rep1")
	_, rep2 := startReplica(t, "localhost:7380", "rep2")
	time.Sleep(20 * time.Millisecond)

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		val := []byte(fmt.Sprintf("val-%d", i))
		if err := p.WrapSet(key, val); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(1 * time.Second)

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		want := fmt.Sprintf("val-%d", i)

		v1, ok1 := rep1.Get(key)
		if !ok1 || string(v1) != want {
			t.Errorf("rep1 key %q: got %q ok=%v, want %q", key, v1, ok1, want)
		}
		v2, ok2 := rep2.Get(key)
		if !ok2 || string(v2) != want {
			t.Errorf("rep2 key %q: got %q ok=%v, want %q", key, v2, ok2, want)
		}
	}
}

func TestReplicaReconnect(t *testing.T) {
	p := startPrimary(t, 7381)
	time.Sleep(20 * time.Millisecond)

	rep, repStore := startReplica(t, "localhost:7381", "rep-reconnect")
	time.Sleep(20 * time.Millisecond)

	for i := 0; i < 10; i++ {
		p.WrapSet(fmt.Sprintf("key-%d", i), []byte("v1"))
	}
	time.Sleep(100 * time.Millisecond)

	// Force-close the replica's connection.
	rep.mu.Lock()
	rep.conn.Close()
	rep.mu.Unlock()

	for i := 10; i < 20; i++ {
		p.WrapSet(fmt.Sprintf("key-%d", i), []byte("v2"))
	}

	// Wait for reconnect (5s delay) plus a bit of sync time.
	time.Sleep(6 * time.Second)

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key-%d", i)
		if _, ok := repStore.Get(key); !ok {
			t.Errorf("replica missing key %q after reconnect", key)
		}
	}
}

func TestReplicaFromBehind(t *testing.T) {
	p := startPrimary(t, 7382)
	time.Sleep(20 * time.Millisecond)

	for i := 0; i < 50; i++ {
		p.WrapSet(fmt.Sprintf("key-%d", i), []byte(fmt.Sprintf("val-%d", i)))
	}

	// Replica connects after all writes; sends lastSeq=0.
	_, repStore := startReplica(t, "localhost:7382", "rep-late")
	time.Sleep(200 * time.Millisecond)

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key-%d", i)
		if _, ok := repStore.Get(key); !ok {
			t.Errorf("late replica missing key %q", key)
		}
	}
}

// --- helpers used by tests above ---

// WALStore doesn't expose the inner store directly; add a thin Get wrapper via
// the public WALStore.Get method (already available).
func (r *Replica) Get(key string) ([]byte, bool) {
	return r.store.Get(key)
}

// startPrimary needs a fixed WAL file path for the reconnect test.
func newWALStoreAt(t *testing.T, path string) *store.WALStore {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	return newWALStoreFile(t, path)
}
