package wal

import (
	"errors"
	"log/slog"
	"time"
)

// StoreApplier is the write side of a store. *store.Store satisfies this interface.
type StoreApplier interface {
	Set(key string, value []byte)
	SetWithTTL(key string, value []byte, ttl time.Duration)
	Delete(key string) bool
}

type RecoveryResult struct {
	EntriesReplayed int
	EntriesSkipped  int
	LastSequence    uint64
	TruncatedAt     uint64
	Error           error
}

func Recover(w *WAL, s StoreApplier) RecoveryResult {
	entries, readErr := w.ReadAll()

	slog.Info("starting WAL recovery", "entries", len(entries))

	var result RecoveryResult

	for _, entry := range entries {
		key := string(entry.Key)
		switch entry.EntryType {
		case EntrySet:
			s.Set(key, entry.Value)
		case EntryDelete:
			s.Delete(key)
		case EntryTTL:
			s.SetWithTTL(key, entry.Value, time.Duration(entry.TTLSeconds)*time.Second)
		default:
			result.EntriesSkipped++
			continue
		}
		result.EntriesReplayed++
		result.LastSequence = entry.SequenceNum
	}

	if errors.Is(readErr, ErrChecksumMismatch) || errors.Is(readErr, ErrTruncated) {
		result.TruncatedAt = result.LastSequence
		slog.Warn("WAL truncated at sequence due to corruption",
			"sequence", result.LastSequence,
			"reason", readErr)
		if err := w.Truncate(result.LastSequence); err != nil {
			result.Error = err
		}
	} else if readErr != nil {
		result.Error = readErr
	}

	slog.Info("WAL recovery complete",
		"replayed", result.EntriesReplayed,
		"last_seq", result.LastSequence)

	return result
}
