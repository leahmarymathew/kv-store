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

type Primary struct {
	store     *store.WALStore
	repBuffer *ReplicationBuffer
	replicas  map[string]net.Conn
	mu        sync.RWMutex
	listener  net.Listener
	port      int
}

func NewPrimary(s *store.WALStore, port int) *Primary {
	return &Primary{
		store:     s,
		repBuffer: NewReplicationBuffer(0),
		replicas:  make(map[string]net.Conn),
		port:      port,
	}
}

func (p *Primary) Start() error {
	ln, err := net.Listen("tcp", addressFromPort(p.port))
	if err != nil {
		return err
	}
	p.listener = ln
	slog.Info("primary replication listener started", "port", p.port)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go p.handleReplica(conn)
		}
	}()
	return nil
}

func (p *Primary) Stop() {
	if p.listener != nil {
		p.listener.Close()
	}
}

func (p *Primary) Addr() string {
	if p.listener == nil {
		return ""
	}
	return p.listener.Addr().String()
}

func (p *Primary) handleReplica(conn net.Conn) {
	defer conn.Close()

	// Read handshake: [8 bytes lastSeq][4 bytes id length][N bytes id]
	var seqBuf [8]byte
	if _, err := io.ReadFull(conn, seqBuf[:]); err != nil {
		slog.Warn("replica handshake seq read failed", "err", err)
		return
	}
	lastSeq := binary.BigEndian.Uint64(seqBuf[:])

	var idLenBuf [4]byte
	if _, err := io.ReadFull(conn, idLenBuf[:]); err != nil {
		slog.Warn("replica handshake id-len read failed", "err", err)
		return
	}
	idLen := binary.BigEndian.Uint32(idLenBuf[:])
	idBytes := make([]byte, idLen)
	if _, err := io.ReadFull(conn, idBytes); err != nil {
		slog.Warn("replica handshake id read failed", "err", err)
		return
	}
	replicaID := string(idBytes)

	p.mu.Lock()
	p.replicas[replicaID] = conn
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		delete(p.replicas, replicaID)
		p.mu.Unlock()
		slog.Info("replica disconnected", "id", replicaID)
	}()

	slog.Info("replica connected", "id", replicaID, "lastSeq", lastSeq)

	entries, needsResync := p.repBuffer.GetSince(lastSeq)

	// lastSeq==0 means the replica has no data at all. If the primary's store
	// also has data that isn't in the buffer (e.g. after a WAL recovery
	// restart that reset the buffer), send a full snapshot so the replica
	// catches up. When the store is empty there is nothing to send and we
	// fall through to the stream loop normally.
	freshAndBehind := lastSeq == 0 && p.store.Len() > 0
	if needsResync || freshAndBehind {
		slog.Info("replica full resync", "id", replicaID, "reason_resync", needsResync, "fresh", freshAndBehind)
		if err := sendFullSync(conn, p.store); err != nil {
			slog.Warn("full sync failed", "id", replicaID, "err", err)
			return
		}
	} else {
		for _, e := range entries {
			if err := sendEntry(conn, e); err != nil {
				slog.Warn("send catchup entry failed", "id", replicaID, "err", err)
				return
			}
		}
	}

	lastSent := p.repBuffer.CurrentSeq()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		newEntries, _ := p.repBuffer.GetSince(lastSent)
		for _, e := range newEntries {
			if err := sendEntry(conn, e); err != nil {
				slog.Warn("stream entry failed", "id", replicaID, "err", err)
				return
			}
			lastSent = e.SequenceNum
		}
	}
}

func sendFullSync(conn net.Conn, s *store.WALStore) error {
	// FULL_SYNC marker: type=0xFF, seq=0, empty key/value/ttl
	marker := ReplicationEntry{EntryType: EntryFull}
	if err := sendEntry(conn, marker); err != nil {
		return err
	}
	for _, key := range s.Keys() {
		val, ok := s.Get(key)
		if !ok {
			continue
		}
		e := ReplicationEntry{
			EntryType: EntrySet,
			Key:       []byte(key),
			Value:     val,
		}
		if err := sendEntry(conn, e); err != nil {
			return err
		}
	}
	return nil
}

// sendEntry writes one entry in the wire format:
// [1 type][8 seq][4 keyLen][key][4 valLen][val][8 ttl]
func sendEntry(conn net.Conn, e ReplicationEntry) error {
	keyLen := uint32(len(e.Key))
	valLen := uint32(len(e.Value))
	size := 1 + 8 + 4 + int(keyLen) + 4 + int(valLen) + 8
	buf := make([]byte, size)

	off := 0
	buf[off] = e.EntryType
	off++
	binary.BigEndian.PutUint64(buf[off:], e.SequenceNum)
	off += 8
	binary.BigEndian.PutUint32(buf[off:], keyLen)
	off += 4
	copy(buf[off:], e.Key)
	off += int(keyLen)
	binary.BigEndian.PutUint32(buf[off:], valLen)
	off += 4
	copy(buf[off:], e.Value)
	off += int(valLen)
	binary.BigEndian.PutUint64(buf[off:], uint64(e.TTLSeconds))

	_, err := conn.Write(buf)
	return err
}

func (p *Primary) WrapSet(key string, value []byte) error {
	if err := p.store.Set(key, value); err != nil {
		return err
	}
	p.repBuffer.Append(EntrySet, []byte(key), value, 0)
	return nil
}

func (p *Primary) WrapSetWithTTL(key string, value []byte, ttl time.Duration) error {
	if err := p.store.SetWithTTL(key, value, ttl); err != nil {
		return err
	}
	p.repBuffer.Append(EntryTTL, []byte(key), value, int64(ttl.Seconds()))
	return nil
}

func (p *Primary) WrapDelete(key string) (bool, error) {
	ok, err := p.store.Delete(key)
	if err != nil {
		return false, err
	}
	p.repBuffer.Append(EntryDelete, []byte(key), nil, 0)
	return ok, nil
}

func (p *Primary) Store() *store.WALStore { return p.store }

func addressFromPort(port int) string {
	return ":" + itoa(port)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	buf := [20]byte{}
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}
