package store

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestTTLManagerSetGet(t *testing.T) {
	m := NewTTLManager()
	expiry := time.Now().Add(time.Second)
	m.Set("k", expiry)

	if m.IsExpired("k") {
		t.Fatal("key should not be expired immediately after Set")
	}

	key, at, ok := m.NextExpiry()
	if !ok {
		t.Fatal("NextExpiry returned nothing")
	}
	if key != "k" {
		t.Errorf("expected key %q, got %q", "k", key)
	}
	if !at.Equal(expiry) {
		t.Errorf("expected expiry %v, got %v", expiry, at)
	}
}

func TestTTLManagerExpired(t *testing.T) {
	m := NewTTLManager()
	m.Set("k", time.Now().Add(50*time.Millisecond))
	time.Sleep(100 * time.Millisecond)
	if !m.IsExpired("k") {
		t.Fatal("key should be expired after TTL elapsed")
	}
}

func TestTTLManagerDelete(t *testing.T) {
	m := NewTTLManager()
	m.Set("k", time.Now().Add(time.Second))
	m.Delete("k")

	if m.IsExpired("k") {
		t.Fatal("deleted key should not appear as expired")
	}
	if got := m.Len(); got != 0 {
		t.Fatalf("expected Len 0 after delete, got %d", got)
	}
}

func TestTTLManagerPopExpired(t *testing.T) {
	m := NewTTLManager()

	for i := 0; i < 5; i++ {
		m.Set(fmt.Sprintf("short%d", i), time.Now().Add(50*time.Millisecond))
	}
	for i := 0; i < 3; i++ {
		m.Set(fmt.Sprintf("long%d", i), time.Now().Add(10*time.Second))
	}

	time.Sleep(100 * time.Millisecond)

	expired := m.PopExpired()
	if len(expired) != 5 {
		t.Fatalf("expected 5 expired keys, got %d: %v", len(expired), expired)
	}
	if got := m.Len(); got != 3 {
		t.Fatalf("expected 3 remaining keys, got %d", got)
	}
}

func TestTTLManagerUpdate(t *testing.T) {
	m := NewTTLManager()
	m.Set("k", time.Now().Add(50*time.Millisecond))
	m.Set("k", time.Now().Add(10*time.Second)) // extend before expiry

	time.Sleep(100 * time.Millisecond)

	if m.IsExpired("k") {
		t.Fatal("key should not be expired after TTL was extended")
	}
	if got := m.Len(); got != 1 {
		t.Fatalf("expected Len 1 (no duplicate entries), got %d", got)
	}
}

func TestTTLManagerMinHeapOrder(t *testing.T) {
	m := NewTTLManager()
	base := time.Now()

	// Insert keys with offsets 1s–10s in shuffled order so heap must reorder them.
	offsets := rand.Perm(10) // permutation of [0, 10)
	for i, off := range offsets {
		m.Set(fmt.Sprintf("key%d", i), base.Add(time.Duration(off+1)*time.Second))
	}

	var prev time.Time
	for i := 0; i < 10; i++ {
		key, at, ok := m.NextExpiry()
		if !ok {
			t.Fatalf("NextExpiry returned nothing at iteration %d", i)
		}
		if i > 0 && at.Before(prev) {
			t.Errorf("heap out of order at iteration %d: got %v, previous was %v", i, at, prev)
		}
		prev = at
		m.Delete(key)
	}

	if got := m.Len(); got != 0 {
		t.Fatalf("expected empty manager after deleting all keys, got %d", got)
	}
}

func TestTTLManagerConcurrent(t *testing.T) {
	m := NewTTLManager()
	var wg sync.WaitGroup
	done := make(chan struct{})

	// 50 goroutines each setting 20 keys with random TTLs.
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				ttl := time.Duration(rand.Intn(400)+100) * time.Millisecond
				m.Set(key, time.Now().Add(ttl))
			}
		}(i)
	}

	// 10 goroutines calling PopExpired in a tight loop until done.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					m.PopExpired()
				}
			}
		}()
	}

	time.Sleep(500 * time.Millisecond)
	close(done)
	wg.Wait()

	if l := m.Len(); l < 0 {
		t.Errorf("Len returned negative value: %d", l)
	}

	// Verify internal heap/index stay in sync (accessible because test is in package store).
	m.mu.Lock()
	heapLen := len(m.heap)
	indexLen := len(m.index)
	m.mu.Unlock()
	if heapLen != indexLen {
		t.Errorf("internal inconsistency: heap len %d != index len %d", heapLen, indexLen)
	}
}
