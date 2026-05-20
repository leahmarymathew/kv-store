package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/leahmarymathew/kv-store/internal/store"
	"github.com/leahmarymathew/kv-store/internal/wal"
	"github.com/stretchr/testify/require"
)

// TestCrashRecovery writes 1000 keys, leaves the WAL open (simulating an
// abrupt process exit), then recovers into a fresh store and verifies every
// key and value survived.
func TestCrashRecovery(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "crash.wal")

	// Step 1: Pre-crash writes.
	ws, err := store.NewWALStoreWithRecovery(walPath)
	require.NoError(t, err)
	// ws is intentionally not closed before recovery — simulates a crash.
	// Deferred close lets t.TempDir() remove the directory on Windows.
	defer ws.Close()

	for i := 0; i < 1000; i++ {
		require.NoError(t, ws.Set(fmt.Sprintf("key:%d", i), []byte(fmt.Sprintf("value:%d", i))))
	}

	// Verify all 1000 keys are readable from the live store before the crash.
	for i := 0; i < 1000; i++ {
		val, ok := ws.Get(fmt.Sprintf("key:%d", i))
		require.True(t, ok, "key:%d should be readable pre-crash", i)
		require.Equal(t, fmt.Sprintf("value:%d", i), string(val))
	}

	// Step 2: Simulate crash — ws is left open, proving recovery works even
	// with a stale concurrent file handle (as on a host restarting in place).

	// Step 3: Recovery using wal.Recover directly to capture RecoveryResult.
	recoveredStore := store.NewStore()
	start := time.Now()
	w2, err := wal.NewWAL(walPath)
	require.NoError(t, err)
	defer w2.Close()

	result := wal.Recover(w2, recoveredStore)
	t.Logf("WAL recovery time: %v", time.Since(start))

	// Step 4: Verify.
	require.NoError(t, result.Error)
	require.Equal(t, 1000, result.EntriesReplayed)

	for i := 0; i < 1000; i++ {
		val, ok := recoveredStore.Get(fmt.Sprintf("key:%d", i))
		require.True(t, ok, "key:%d should exist after recovery", i)
		require.Equal(t, fmt.Sprintf("value:%d", i), string(val))
	}
}

// TestCrashRecoveryWithDeletes verifies that delete operations applied before
// a crash are correctly reflected in the recovered store.
func TestCrashRecoveryWithDeletes(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "deletes.wal")

	ws, err := store.NewWALStoreWithRecovery(walPath)
	require.NoError(t, err)
	defer ws.Close()

	for _, key := range []string{"a", "b", "c", "d", "e"} {
		require.NoError(t, ws.Set(key, []byte("val")))
	}
	for _, key := range []string{"b", "d"} {
		_, err := ws.Delete(key)
		require.NoError(t, err)
	}
	// Simulate crash: ws not closed before recovery opens the same file path.

	ws2, err := store.NewWALStoreWithRecovery(walPath)
	require.NoError(t, err)
	defer ws2.Close()

	for _, key := range []string{"a", "c", "e"} {
		_, ok := ws2.Get(key)
		require.True(t, ok, "key %q should survive recovery", key)
	}
	for _, key := range []string{"b", "d"} {
		_, ok := ws2.Get(key)
		require.False(t, ok, "deleted key %q should not appear after recovery", key)
	}
}

// TestCrashRecoveryWithTTL verifies that a key written with a TTL survives
// recovery and still has an active expiry.
func TestCrashRecoveryWithTTL(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "ttl.wal")

	ws, err := store.NewWALStoreWithRecovery(walPath)
	require.NoError(t, err)
	defer ws.Close()

	require.NoError(t, ws.SetWithTTL("ttlkey", []byte("ttlval"), 10*time.Second))
	// Simulate crash.

	// Recovery: TTL is reset to 10 s from the recovery instant (WAL stores the
	// duration, not the absolute expiry time, so the clock restarts on replay).
	ws2, err := store.NewWALStoreWithRecovery(walPath)
	require.NoError(t, err)
	defer ws2.Close()

	val, ok := ws2.Get("ttlkey")
	require.True(t, ok, "key should be present immediately after recovery")
	require.Equal(t, []byte("ttlval"), val)

	// TTL is still active: key is accessible well within the 10-second window.
	time.Sleep(50 * time.Millisecond)
	_, ok = ws2.Get("ttlkey")
	require.True(t, ok, "key should still be present 50 ms after recovery (TTL is 10 s)")
}

// TestPartialWriteRecovery simulates a crash that left the last WAL entry
// half-written. Recovery must replay the 99 intact entries, truncate the
// corrupt tail, and leave the file fully clean.
func TestPartialWriteRecovery(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "partial.wal")

	w, err := wal.NewWAL(walPath)
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		_, err = w.Append(wal.EntrySet,
			[]byte(fmt.Sprintf("pk%d", i)),
			[]byte(fmt.Sprintf("pv%d", i)),
			0)
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())

	// Lop the last 5 bytes off the file, cutting the 100th entry mid-stream.
	info, err := os.Stat(walPath)
	require.NoError(t, err)
	require.NoError(t, os.Truncate(walPath, info.Size()-5))

	// Recover — should replay 99 entries and truncate the corrupt tail.
	s := store.NewStore()
	w2, err := wal.NewWAL(walPath)
	require.NoError(t, err)
	defer w2.Close()

	result := wal.Recover(w2, s)
	require.NoError(t, result.Error)
	require.Equal(t, 99, result.EntriesReplayed)
	require.NotZero(t, result.TruncatedAt)

	// A second ReadAll must return exactly 99 clean entries with no error.
	entries, err := w2.ReadAll()
	require.NoError(t, err)
	require.Len(t, entries, 99)
}

// BenchmarkWALWrite measures raw WAL append throughput.
func BenchmarkWALWrite(b *testing.B) {
	w, err := wal.NewWAL(filepath.Join(b.TempDir(), "bench.wal"))
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := w.Append(wal.EntrySet, []byte("benchkey"), []byte("benchvalue"), 0); err != nil {
			b.Fatal(err)
		}
	}
}
