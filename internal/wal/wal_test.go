package wal_test

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/leahmarymathew/kv-store/internal/store"
	"github.com/leahmarymathew/kv-store/internal/wal"
	"github.com/stretchr/testify/require"
)

func TestWALAppendAndRead(t *testing.T) {
	w, err := wal.NewWAL(filepath.Join(t.TempDir(), "wal.log"))
	require.NoError(t, err)
	defer w.Close()

	_, err = w.Append(wal.EntrySet, []byte("foo"), []byte("bar"), 0)
	require.NoError(t, err)
	_, err = w.Append(wal.EntrySet, []byte("baz"), []byte("qux"), 0)
	require.NoError(t, err)
	_, err = w.Append(wal.EntryDelete, []byte("foo"), nil, 0)
	require.NoError(t, err)

	entries, err := w.ReadAll()
	require.NoError(t, err)
	require.Len(t, entries, 3)

	require.Equal(t, uint64(1), entries[0].SequenceNum)
	require.Equal(t, wal.EntrySet, entries[0].EntryType)
	require.Equal(t, []byte("foo"), entries[0].Key)
	require.Equal(t, []byte("bar"), entries[0].Value)

	require.Equal(t, uint64(2), entries[1].SequenceNum)
	require.Equal(t, wal.EntrySet, entries[1].EntryType)
	require.Equal(t, []byte("baz"), entries[1].Key)
	require.Equal(t, []byte("qux"), entries[1].Value)

	require.Equal(t, uint64(3), entries[2].SequenceNum)
	require.Equal(t, wal.EntryDelete, entries[2].EntryType)
	require.Equal(t, []byte("foo"), entries[2].Key)
}

func TestWALChecksum(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	w, err := wal.NewWAL(path)
	require.NoError(t, err)
	_, err = w.Append(wal.EntrySet, []byte("key"), []byte("val"), 0)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	// Flip byte at offset 5 (inside the sequence number field) to corrupt the entry.
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	require.NoError(t, err)
	var b [1]byte
	_, err = f.ReadAt(b[:], 5)
	require.NoError(t, err)
	b[0] ^= 0xFF
	_, err = f.WriteAt(b[:], 5)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	w2, err := wal.NewWAL(path)
	require.NoError(t, err)
	defer w2.Close()

	entries, err := w2.ReadAll()
	require.ErrorIs(t, err, wal.ErrChecksumMismatch)
	require.Empty(t, entries)
}

func TestWALTruncatedEntry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	w, err := wal.NewWAL(path)
	require.NoError(t, err)
	// Each entry: seq(8)+type(1)+keylen(4)+key(4)+valuelen(4)+value(4)+ttl(8)+sum(4) = 37 bytes.
	_, err = w.Append(wal.EntrySet, []byte("key1"), []byte("val1"), 0)
	require.NoError(t, err)
	_, err = w.Append(wal.EntrySet, []byte("key2"), []byte("val2"), 0)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.NoError(t, os.Truncate(path, info.Size()-3))

	w2, err := wal.NewWAL(path)
	require.NoError(t, err)
	defer w2.Close()

	entries, err := w2.ReadAll()
	require.ErrorIs(t, err, wal.ErrTruncated)
	require.Len(t, entries, 1)
	require.Equal(t, []byte("key1"), entries[0].Key)
	require.Equal(t, []byte("val1"), entries[0].Value)
}

func TestWALSequenceNumbersPersist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	w, err := wal.NewWAL(path)
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		_, err = w.Append(wal.EntrySet, []byte("k"), []byte("v"), 0)
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())

	w2, err := wal.NewWAL(path)
	require.NoError(t, err)
	defer w2.Close()

	seq, err := w2.Append(wal.EntrySet, []byte("k"), []byte("v"), 0)
	require.NoError(t, err)
	require.Equal(t, uint64(6), seq)
}

func TestWALRecovery(t *testing.T) {
	w, err := wal.NewWAL(filepath.Join(t.TempDir(), "wal.log"))
	require.NoError(t, err)
	defer w.Close()

	_, err = w.Append(wal.EntrySet, []byte("a"), []byte("1"), 0)
	require.NoError(t, err)
	_, err = w.Append(wal.EntrySet, []byte("b"), []byte("2"), 0)
	require.NoError(t, err)
	_, err = w.Append(wal.EntrySet, []byte("c"), []byte("3"), 0)
	require.NoError(t, err)

	s := store.NewStore()
	result := wal.Recover(w, s)

	require.NoError(t, result.Error)
	require.Equal(t, 3, result.EntriesReplayed)
	require.Zero(t, result.TruncatedAt)

	v, ok := s.Get("a")
	require.True(t, ok)
	require.Equal(t, []byte("1"), v)

	v, ok = s.Get("b")
	require.True(t, ok)
	require.Equal(t, []byte("2"), v)

	v, ok = s.Get("c")
	require.True(t, ok)
	require.Equal(t, []byte("3"), v)
}

func TestWALRecoveryWithDelete(t *testing.T) {
	w, err := wal.NewWAL(filepath.Join(t.TempDir(), "wal.log"))
	require.NoError(t, err)
	defer w.Close()

	_, err = w.Append(wal.EntrySet, []byte("a"), []byte("1"), 0)
	require.NoError(t, err)
	_, err = w.Append(wal.EntrySet, []byte("b"), []byte("2"), 0)
	require.NoError(t, err)
	_, err = w.Append(wal.EntryDelete, []byte("a"), nil, 0)
	require.NoError(t, err)

	s := store.NewStore()
	result := wal.Recover(w, s)

	require.NoError(t, result.Error)
	require.Equal(t, 3, result.EntriesReplayed)

	_, ok := s.Get("a")
	require.False(t, ok, "key 'a' should have been deleted during recovery")

	v, ok := s.Get("b")
	require.True(t, ok)
	require.Equal(t, []byte("2"), v)
}

func TestWALRecoveryCorrupted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	w, err := wal.NewWAL(path)
	require.NoError(t, err)
	// Each entry: seq(8)+type(1)+keylen(4)+key(1)+valuelen(4)+value(1)+ttl(8)+sum(4) = 31 bytes.
	_, err = w.Append(wal.EntrySet, []byte("a"), []byte("1"), 0)
	require.NoError(t, err)
	_, err = w.Append(wal.EntrySet, []byte("b"), []byte("2"), 0)
	require.NoError(t, err)
	_, err = w.Append(wal.EntrySet, []byte("c"), []byte("3"), 0)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	// Corrupt byte at offset 5 inside entry 3 (entry 3 starts at 2*31=62).
	const entrySize = 31
	corruptOffset := int64(2*entrySize + 5)

	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	require.NoError(t, err)
	var b [1]byte
	_, err = f.ReadAt(b[:], corruptOffset)
	require.NoError(t, err)
	b[0] ^= 0xFF
	_, err = f.WriteAt(b[:], corruptOffset)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	w2, err := wal.NewWAL(path)
	require.NoError(t, err)
	defer w2.Close()

	s := store.NewStore()
	result := wal.Recover(w2, s)

	require.NoError(t, result.Error)
	require.Equal(t, 2, result.EntriesReplayed)
	require.NotZero(t, result.TruncatedAt)

	// After truncation the WAL must contain exactly 2 clean entries.
	entries, err := w2.ReadAll()
	require.NoError(t, err)
	require.Len(t, entries, 2)
}

func TestWALConcurrentAppend(t *testing.T) {
	w, err := wal.NewWAL(filepath.Join(t.TempDir(), "wal.log"))
	require.NoError(t, err)
	defer w.Close()

	const goroutines = 20
	const perGoroutine = 50

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				if _, err := w.Append(wal.EntrySet, []byte("k"), []byte("v"), 0); err != nil {
					t.Errorf("Append failed: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	entries, err := w.ReadAll()
	require.NoError(t, err)
	require.Len(t, entries, goroutines*perGoroutine)

	seen := make(map[uint64]struct{}, len(entries))
	for _, e := range entries {
		_, dup := seen[e.SequenceNum]
		require.False(t, dup, "duplicate sequence number %d", e.SequenceNum)
		seen[e.SequenceNum] = struct{}{}
	}
}
