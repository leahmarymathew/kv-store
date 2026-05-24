package cluster

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
)

type NodeID string

type virtualNode struct {
	hash   uint32
	nodeID NodeID
}

type HashRing struct {
	ring     []virtualNode
	nodes    map[NodeID]bool
	replicas int
	mu       sync.RWMutex
}

func NewHashRing(replicas int) *HashRing {
	if replicas == 0 {
		replicas = 150
	}
	return &HashRing{
		nodes:    make(map[NodeID]bool),
		replicas: replicas,
	}
}

func (h *HashRing) AddNode(id NodeID) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for i := 0; i < h.replicas; i++ {
		key := fmt.Sprintf("%s#%d", id, i)
		hash := crc32.ChecksumIEEE([]byte(key))
		h.ring = append(h.ring, virtualNode{hash, id})
	}
	sort.Slice(h.ring, func(i, j int) bool {
		return h.ring[i].hash < h.ring[j].hash
	})
	h.nodes[id] = true
}

func (h *HashRing) RemoveNode(id NodeID) {
	h.mu.Lock()
	defer h.mu.Unlock()

	filtered := h.ring[:0]
	for _, vn := range h.ring {
		if vn.nodeID != id {
			filtered = append(filtered, vn)
		}
	}
	h.ring = filtered
	delete(h.nodes, id)
}

func (h *HashRing) GetNode(key string) (NodeID, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.ring) == 0 {
		return "", false
	}
	hash := crc32.ChecksumIEEE([]byte(key))
	idx := sort.Search(len(h.ring), func(i int) bool {
		return h.ring[i].hash >= hash
	})
	if idx == len(h.ring) {
		idx = 0
	}
	return h.ring[idx].nodeID, true
}

func (h *HashRing) GetNodes(key string, n int) []NodeID {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.ring) == 0 || n == 0 {
		return nil
	}
	if n > len(h.nodes) {
		n = len(h.nodes)
	}

	hash := crc32.ChecksumIEEE([]byte(key))
	idx := sort.Search(len(h.ring), func(i int) bool {
		return h.ring[i].hash >= hash
	})
	if idx == len(h.ring) {
		idx = 0
	}

	seen := make(map[NodeID]bool)
	result := make([]NodeID, 0, n)
	for i := 0; len(result) < n; i++ {
		vn := h.ring[(idx+i)%len(h.ring)]
		if !seen[vn.nodeID] {
			seen[vn.nodeID] = true
			result = append(result, vn.nodeID)
		}
	}
	return result
}

func (h *HashRing) Nodes() []NodeID {
	h.mu.RLock()
	defer h.mu.RUnlock()

	ids := make([]NodeID, 0, len(h.nodes))
	for id := range h.nodes {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	return ids
}

func (h *HashRing) Len() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.nodes)
}
