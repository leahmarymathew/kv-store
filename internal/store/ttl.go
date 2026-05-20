package store

import (
	"container/heap"
	"sync"
	"time"
)

type ttlEntry struct {
	key       string
	expiresAt time.Time
}

// ttlHeap is a min-heap of ttlEntry ordered by expiresAt (earliest first).
type ttlHeap []ttlEntry

func (h ttlHeap) Len() int            { return len(h) }
func (h ttlHeap) Less(i, j int) bool  { return h[i].expiresAt.Before(h[j].expiresAt) }
func (h ttlHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }

func (h *ttlHeap) Push(x interface{}) {
	*h = append(*h, x.(ttlEntry))
}

func (h *ttlHeap) Pop() interface{} {
	old := *h
	n := len(old)
	entry := old[n-1]
	*h = old[:n-1]
	return entry
}

// TTLManager tracks per-key expiry using a min-heap for O(log n) set/delete
// and O(k log n) batch expiry collection.
type TTLManager struct {
	heap  ttlHeap
	index map[string]int
	mu    sync.Mutex
}

func NewTTLManager() *TTLManager {
	m := &TTLManager{
		heap:  make(ttlHeap, 0),
		index: make(map[string]int),
	}
	heap.Init(&m.heap)
	return m
}

// rebuildIndex scans the heap and refreshes index positions.
// Must be called after any operation that reorders heap elements.
func (m *TTLManager) rebuildIndex() {
	for i, e := range m.heap {
		m.index[e.key] = i
	}
}

// Set adds or updates the expiry time for key.
func (m *TTLManager) Set(key string, expiresAt time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if idx, ok := m.index[key]; ok {
		m.heap[idx].expiresAt = expiresAt
		heap.Fix(&m.heap, idx)
	} else {
		heap.Push(&m.heap, ttlEntry{key: key, expiresAt: expiresAt})
	}
	m.rebuildIndex()
}

// Delete removes key from the TTL manager if present.
func (m *TTLManager) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	idx, ok := m.index[key]
	if !ok {
		return
	}
	heap.Remove(&m.heap, idx)
	delete(m.index, key)
	m.rebuildIndex()
}

// IsExpired returns true if key has a TTL set and it has passed.
// Returns false if key has no TTL.
func (m *TTLManager) IsExpired(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	idx, ok := m.index[key]
	if !ok {
		return false
	}
	return time.Now().After(m.heap[idx].expiresAt)
}

// NextExpiry returns the key with the soonest expiry without removing it.
func (m *TTLManager) NextExpiry() (key string, expiresAt time.Time, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.heap) == 0 {
		return "", time.Time{}, false
	}
	return m.heap[0].key, m.heap[0].expiresAt, true
}

// PopExpired removes and returns all keys whose expiry time is <= now.
func (m *TTLManager) PopExpired() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	var expired []string
	for len(m.heap) > 0 && !m.heap[0].expiresAt.After(now) {
		entry := heap.Pop(&m.heap).(ttlEntry)
		delete(m.index, entry.key)
		expired = append(expired, entry.key)
	}
	m.rebuildIndex()
	return expired
}

// Len returns the number of keys currently tracked.
func (m *TTLManager) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.heap)
}
