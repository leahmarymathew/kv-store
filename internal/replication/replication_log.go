package replication

import "sync"

const (
	EntrySet    uint8 = 0x01
	EntryDelete uint8 = 0x02
	EntryTTL    uint8 = 0x03
	EntryFull   uint8 = 0xFF
)

type ReplicationEntry struct {
	SequenceNum uint64
	EntryType   uint8
	Key         []byte
	Value       []byte
	TTLSeconds  int64
}

type ReplicationBuffer struct {
	entries  []ReplicationEntry
	head     int
	tail     int
	count    int
	capacity int
	mu       sync.RWMutex
	seq      uint64
}

func NewReplicationBuffer(capacity int) *ReplicationBuffer {
	if capacity == 0 {
		capacity = 10000
	}
	return &ReplicationBuffer{
		entries:  make([]ReplicationEntry, capacity),
		capacity: capacity,
	}
}

func (rb *ReplicationBuffer) Append(entryType uint8, key, value []byte, ttlSeconds int64) uint64 {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	rb.seq++
	rb.entries[rb.tail] = ReplicationEntry{
		SequenceNum: rb.seq,
		EntryType:   entryType,
		Key:         key,
		Value:       value,
		TTLSeconds:  ttlSeconds,
	}
	rb.tail = (rb.tail + 1) % rb.capacity
	if rb.count == rb.capacity {
		rb.head = (rb.head + 1) % rb.capacity
	} else {
		rb.count++
	}
	return rb.seq
}

// GetSince returns all entries with SequenceNum > afterSeq.
// needsResync is true when afterSeq has already been overwritten.
func (rb *ReplicationBuffer) GetSince(afterSeq uint64) ([]ReplicationEntry, bool) {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.count == 0 {
		return nil, false
	}

	oldestSeq := rb.entries[rb.head].SequenceNum
	needsResync := afterSeq > 0 && afterSeq < oldestSeq-1

	var result []ReplicationEntry
	for i := 0; i < rb.count; i++ {
		e := rb.entries[(rb.head+i)%rb.capacity]
		if e.SequenceNum > afterSeq {
			result = append(result, e)
		}
	}
	return result, needsResync
}

func (rb *ReplicationBuffer) CurrentSeq() uint64 {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return rb.seq
}
