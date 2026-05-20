package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

type LogEntryType uint8

const (
	EntrySet    LogEntryType = 0x01
	EntryDelete LogEntryType = 0x02
	EntryTTL    LogEntryType = 0x03
)

var (
	ErrChecksumMismatch = errors.New("WAL checksum mismatch")
	ErrTruncated        = errors.New("WAL entry truncated")
)

type LogEntry struct {
	SequenceNum uint64
	EntryType   LogEntryType
	Key         []byte
	Value       []byte
	TTLSeconds  int64
	Checksum    uint32
}

type WAL struct {
	file     *os.File
	mu       sync.Mutex
	seq      uint64
	filePath string
}

func NewWAL(filePath string) (*WAL, error) {
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	w := &WAL{file: f, filePath: filePath}
	if err := w.findLastSequence(); err != nil {
		f.Close()
		return nil, err
	}
	return w, nil
}

// findLastSequence scans existing entries to set seq to the highest seen.
func (w *WAL) findLastSequence() error {
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	for {
		entry, err := readEntry(w.file)
		if err == io.EOF {
			return nil
		}
		if errors.Is(err, ErrChecksumMismatch) || errors.Is(err, ErrTruncated) {
			// Stop at corruption; seq stays at last valid value.
			return nil
		}
		if err != nil {
			return err
		}
		if entry.SequenceNum > w.seq {
			w.seq = entry.SequenceNum
		}
	}
}

func (w *WAL) Append(entryType LogEntryType, key, value []byte, ttlSeconds int64) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.seq++
	seq := w.seq

	buf := marshalEntry(seq, entryType, key, value, ttlSeconds)

	if _, err := w.file.Write(buf); err != nil {
		return 0, err
	}
	if err := w.file.Sync(); err != nil {
		return 0, err
	}
	return seq, nil
}

func (w *WAL) ReadAll() ([]LogEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	var entries []LogEntry
	for {
		entry, err := readEntry(w.file)
		if err == io.EOF {
			return entries, nil
		}
		if errors.Is(err, ErrChecksumMismatch) {
			return entries, ErrChecksumMismatch
		}
		if errors.Is(err, ErrTruncated) {
			return entries, ErrTruncated
		}
		if err != nil {
			return entries, err
		}
		entries = append(entries, entry)
	}
}

func (w *WAL) Truncate(afterSeq uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	var offset int64
	for {
		start, err := w.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		entry, err := readEntry(w.file)
		if err == io.EOF {
			break
		}
		if errors.Is(err, ErrChecksumMismatch) || errors.Is(err, ErrTruncated) {
			// Truncate at the start of the corrupt entry.
			return os.Truncate(w.filePath, start)
		}
		if err != nil {
			return err
		}
		if entry.SequenceNum <= afterSeq {
			// Advance offset to end of this valid entry.
			offset, err = w.file.Seek(0, io.SeekCurrent)
			if err != nil {
				return err
			}
		} else {
			// First entry past afterSeq — truncate here.
			return os.Truncate(w.filePath, start)
		}
	}
	return os.Truncate(w.filePath, offset)
}

func (w *WAL) Close() error {
	return w.file.Close()
}

// marshalEntry encodes a log entry into a single byte slice including checksum.
func marshalEntry(seq uint64, entryType LogEntryType, key, value []byte, ttlSeconds int64) []byte {
	keyLen := uint32(len(key))
	valLen := uint32(len(value))

	// 8 + 1 + 4 + N + 4 + M + 8 + 4
	size := 8 + 1 + 4 + int(keyLen) + 4 + int(valLen) + 8 + 4
	buf := make([]byte, size)

	off := 0
	binary.BigEndian.PutUint64(buf[off:], seq)
	off += 8
	buf[off] = byte(entryType)
	off++
	binary.BigEndian.PutUint32(buf[off:], keyLen)
	off += 4
	copy(buf[off:], key)
	off += int(keyLen)
	binary.BigEndian.PutUint32(buf[off:], valLen)
	off += 4
	copy(buf[off:], value)
	off += int(valLen)
	binary.BigEndian.PutUint64(buf[off:], uint64(ttlSeconds))
	off += 8

	checksum := crc32.Checksum(buf[:off], crc32.IEEETable)
	binary.BigEndian.PutUint32(buf[off:], checksum)

	return buf
}

// readEntry reads and validates one log entry from r.
func readEntry(r io.Reader) (LogEntry, error) {
	var header [8 + 1 + 4]byte // seq(8) + type(1) + keylen(4)
	if _, err := io.ReadFull(r, header[:]); err != nil {
		if err == io.EOF {
			return LogEntry{}, io.EOF
		}
		return LogEntry{}, ErrTruncated
	}

	seq := binary.BigEndian.Uint64(header[0:8])
	entryType := LogEntryType(header[8])
	keyLen := binary.BigEndian.Uint32(header[9:13])

	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return LogEntry{}, ErrTruncated
	}

	var valLenBuf [4]byte
	if _, err := io.ReadFull(r, valLenBuf[:]); err != nil {
		return LogEntry{}, ErrTruncated
	}
	valLen := binary.BigEndian.Uint32(valLenBuf[:])

	value := make([]byte, valLen)
	if _, err := io.ReadFull(r, value); err != nil {
		return LogEntry{}, ErrTruncated
	}

	var ttlBuf [8]byte
	if _, err := io.ReadFull(r, ttlBuf[:]); err != nil {
		return LogEntry{}, ErrTruncated
	}
	ttlSeconds := int64(binary.BigEndian.Uint64(ttlBuf[:]))

	var checksumBuf [4]byte
	if _, err := io.ReadFull(r, checksumBuf[:]); err != nil {
		return LogEntry{}, ErrTruncated
	}
	storedChecksum := binary.BigEndian.Uint32(checksumBuf[:])

	// Recompute checksum over everything before the checksum field.
	h := crc32.NewIEEE()
	h.Write(header[:])
	h.Write(key)
	h.Write(valLenBuf[:])
	h.Write(value)
	h.Write(ttlBuf[:])
	if h.Sum32() != storedChecksum {
		return LogEntry{}, ErrChecksumMismatch
	}

	return LogEntry{
		SequenceNum: seq,
		EntryType:   entryType,
		Key:         key,
		Value:       value,
		TTLSeconds:  ttlSeconds,
		Checksum:    storedChecksum,
	}, nil
}
