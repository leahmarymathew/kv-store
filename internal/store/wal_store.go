package store

import (
	"context"
	"log/slog"
	"time"

	"github.com/leahmarymathew/kv-store/internal/wal"
)

type WALStore struct {
	store *Store
	wal   *wal.WAL
}

func NewWALStore(s *Store, w *wal.WAL) *WALStore {
	return &WALStore{store: s, wal: w}
}

func NewWALStoreWithRecovery(filePath string) (*WALStore, error) {
	s := NewStore()
	w, err := wal.NewWAL(filePath)
	if err != nil {
		return nil, err
	}
	result := wal.Recover(w, s)
	slog.Info("WAL store ready",
		"replayed", result.EntriesReplayed,
		"skipped", result.EntriesSkipped,
		"last_seq", result.LastSequence,
		"truncated_at", result.TruncatedAt,
	)
	if result.Error != nil {
		return nil, result.Error
	}
	return NewWALStore(s, w), nil
}

func (ws *WALStore) Get(key string) ([]byte, bool) {
	return ws.store.Get(key)
}

func (ws *WALStore) Set(key string, value []byte) error {
	if _, err := ws.wal.Append(wal.EntrySet, []byte(key), value, 0); err != nil {
		return err
	}
	ws.store.Set(key, value)
	return nil
}

func (ws *WALStore) SetWithTTL(key string, value []byte, ttl time.Duration) error {
	if _, err := ws.wal.Append(wal.EntryTTL, []byte(key), value, int64(ttl.Seconds())); err != nil {
		return err
	}
	ws.store.SetWithTTL(key, value, ttl)
	return nil
}

func (ws *WALStore) Delete(key string) (bool, error) {
	if _, err := ws.wal.Append(wal.EntryDelete, []byte(key), nil, 0); err != nil {
		return false, err
	}
	return ws.store.Delete(key), nil
}

func (ws *WALStore) Len() int {
	return ws.store.Len()
}

func (ws *WALStore) Keys() []string {
	return ws.store.Keys()
}

func (ws *WALStore) StartExpiryLoop(ctx context.Context, interval time.Duration) {
	ws.store.StartExpiryLoop(ctx, interval)
}

func (ws *WALStore) Close() error {
	return ws.wal.Close()
}
