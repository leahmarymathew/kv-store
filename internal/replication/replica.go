package replication

import (
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/leahmarymathew/kv-store/internal/store"
)

type Replica struct {
	store       *store.WALStore
	primaryAddr string
	replicaID   string
	lastSeq     uint64
	conn        net.Conn
	mu          sync.Mutex
}

func NewReplica(s *store.WALStore, primaryAddr, replicaID string) *Replica {
	return &Replica{
		store:       s,
		primaryAddr: primaryAddr,
		replicaID:   replicaID,
	}
}

func (r *Replica) Connect() error {
	conn, err := net.Dial("tcp", r.primaryAddr)
	if err != nil {
		return err
	}

	// Handshake: [8 bytes lastSeq][4 bytes id length][N bytes id]
	idBytes := []byte(r.replicaID)
	buf := make([]byte, 8+4+len(idBytes))
	binary.BigEndian.PutUint64(buf[0:], r.lastSeq)
	binary.BigEndian.PutUint32(buf[8:], uint32(len(idBytes)))
	copy(buf[12:], idBytes)

	if _, err := conn.Write(buf); err != nil {
		conn.Close()
		return err
	}

	r.mu.Lock()
	r.conn = conn
	r.mu.Unlock()

	slog.Info("replica connected to primary", "id", r.replicaID, "addr", r.primaryAddr, "lastSeq", r.lastSeq)
	return nil
}

func (r *Replica) StartSync() error {
	if err := r.Connect(); err != nil {
		return err
	}
	go r.receiveLoop()
	return nil
}

func (r *Replica) receiveLoop() {
	for {
		r.mu.Lock()
		conn := r.conn
		r.mu.Unlock()

		err := r.readEntries(conn)
		slog.Warn("replica disconnected from primary", "id", r.replicaID, "err", err)

		time.Sleep(5 * time.Second)

		if err := r.Connect(); err != nil {
			slog.Warn("replica reconnect failed", "id", r.replicaID, "err", err)
		}
	}
}

func (r *Replica) readEntries(conn net.Conn) error {
	inFullSync := false

	for {
		// [1 type][8 seq][4 keyLen][key][4 valLen][val][8 ttl]
		var header [13]byte // type(1) + seq(8) + keyLen(4)
		if _, err := io.ReadFull(conn, header[:]); err != nil {
			return err
		}

		entryType := header[0]
		seq := binary.BigEndian.Uint64(header[1:9])
		keyLen := binary.BigEndian.Uint32(header[9:13])

		key := make([]byte, keyLen)
		if _, err := io.ReadFull(conn, key); err != nil {
			return err
		}

		var valLenBuf [4]byte
		if _, err := io.ReadFull(conn, valLenBuf[:]); err != nil {
			return err
		}
		valLen := binary.BigEndian.Uint32(valLenBuf[:])

		val := make([]byte, valLen)
		if _, err := io.ReadFull(conn, val); err != nil {
			return err
		}

		var ttlBuf [8]byte
		if _, err := io.ReadFull(conn, ttlBuf[:]); err != nil {
			return err
		}
		ttl := int64(binary.BigEndian.Uint64(ttlBuf[:]))

		switch entryType {
		case EntryFull:
			slog.Info("replica starting full resync", "id", r.replicaID)
			// Clear local store by deleting all keys.
			for _, k := range r.store.Keys() {
				r.store.Delete(k) //nolint:errcheck
			}
			inFullSync = true
			_ = inFullSync
			continue

		case EntrySet:
			if err := r.store.Set(string(key), val); err != nil {
				slog.Warn("replica Set failed", "err", err)
			}

		case EntryDelete:
			r.store.Delete(string(key)) //nolint:errcheck

		case EntryTTL:
			d := time.Duration(ttl) * time.Second
			if err := r.store.SetWithTTL(string(key), val, d); err != nil {
				slog.Warn("replica SetWithTTL failed", "err", err)
			}
		}

		if seq > 0 {
			r.mu.Lock()
			r.lastSeq = seq
			r.mu.Unlock()
		}

		slog.Debug("replica applied entry", "id", r.replicaID, "type", entryType, "seq", seq, "key", string(key))
	}
}

func (r *Replica) LastSeq() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastSeq
}
