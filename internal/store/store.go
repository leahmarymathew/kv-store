package store

import (
	"context"
	"sync"
	"time"
)

type Store struct {
	mu   sync.RWMutex
	data map[string][]byte
	ttl  map[string]time.Time
}

func NewStore() *Store {
	return &Store{
		data: make(map[string][]byte),
		ttl:  make(map[string]time.Time),
	}
}

func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	expiry, hasTTL := s.ttl[key]
	if hasTTL && time.Now().After(expiry) {
		s.mu.RUnlock()
		s.mu.Lock()
		// Double-check: another goroutine may have already cleaned up.
		if exp, still := s.ttl[key]; still && time.Now().After(exp) {
			delete(s.data, key)
			delete(s.ttl, key)
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
	delete(s.ttl, key)
}

func (s *Store) SetWithTTL(key string, value []byte, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
	s.ttl[key] = time.Now().Add(ttl)
}

func (s *Store) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, exists := s.data[key]
	delete(s.data, key)
	delete(s.ttl, key)
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
	now := time.Now()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		if expiry, hasTTL := s.ttl[k]; hasTTL && now.After(expiry) {
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
			case now := <-ticker.C:
				s.mu.Lock()
				for k, expiry := range s.ttl {
					if now.After(expiry) {
						delete(s.data, k)
						delete(s.ttl, k)
					}
				}
				s.mu.Unlock()
			}
		}
	}()
}
