package cluster

import "sync"

type NodeAddress struct {
	ID   NodeID
	Host string
	Port int
}

type Router struct {
	ring      *HashRing
	addresses map[NodeID]NodeAddress
	mu        sync.RWMutex
}

func NewRouter(replicas int) *Router {
	return &Router{
		ring:      NewHashRing(replicas),
		addresses: make(map[NodeID]NodeAddress),
	}
}

func (r *Router) RegisterNode(addr NodeAddress) {
	r.mu.Lock()
	r.addresses[addr.ID] = addr
	r.mu.Unlock()
	r.ring.AddNode(addr.ID)
}

func (r *Router) DeregisterNode(id NodeID) {
	r.ring.RemoveNode(id)
	r.mu.Lock()
	delete(r.addresses, id)
	r.mu.Unlock()
}

func (r *Router) RouteKey(key string) (NodeAddress, bool) {
	id, ok := r.ring.GetNode(key)
	if !ok {
		return NodeAddress{}, false
	}
	r.mu.RLock()
	addr, found := r.addresses[id]
	r.mu.RUnlock()
	return addr, found
}

func (r *Router) RouteKeyToN(key string, n int) []NodeAddress {
	ids := r.ring.GetNodes(key, n)
	r.mu.RLock()
	defer r.mu.RUnlock()
	addrs := make([]NodeAddress, 0, len(ids))
	for _, id := range ids {
		if addr, ok := r.addresses[id]; ok {
			addrs = append(addrs, addr)
		}
	}
	return addrs
}

func (r *Router) IsLocal(key string, localID NodeID) bool {
	id, ok := r.ring.GetNode(key)
	return ok && id == localID
}

func (r *Router) AddressFor(id NodeID) (NodeAddress, bool) {
	r.mu.RLock()
	addr, ok := r.addresses[id]
	r.mu.RUnlock()
	return addr, ok
}
