package store

import (
	"context"
	"sync"
	"time"
)

type Store struct {
	mu     sync.RWMutex
	data   map[string][]byte
	ttlMgr *TTLManager
}

func NewStore() *Store {
	return &Store{
		data:   make(map[string][]byte),
		ttlMgr: NewTTLManager(),
	}
}

func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	if s.ttlMgr.IsExpired(key) {
		s.mu.RUnlock()
		s.mu.Lock()
		// Double-check: another goroutine may have already cleaned up or
		// replaced the key with a fresh Set() clearing the TTL.
		if s.ttlMgr.IsExpired(key) {
			delete(s.data, key)
			s.ttlMgr.Delete(key)
		}
		s.mu.Unlock()
		return nil, false
	}
	val, ok := s.data[key]
	s.mu.RUnlock()
	return val, ok
}

func (s *Store) Set(key string, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
	s.ttlMgr.Delete(key)
}

func (s *Store) SetWithTTL(key string, value []byte, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
	s.ttlMgr.Set(key, time.Now().Add(ttl))
}

func (s *Store) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, exists := s.data[key]
	delete(s.data, key)
	s.ttlMgr.Delete(key)
	return exists
}

func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		if s.ttlMgr.IsExpired(k) {
			continue
		}
		keys = append(keys, k)
	}
	return keys
}

func (s *Store) StartExpiryLoop(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				expired := s.ttlMgr.PopExpired()
				for _, key := range expired {
					s.mu.Lock()
					delete(s.data, key)
					s.mu.Unlock()
				}
			}
		}
	}()
}
