package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetGet(t *testing.T) {
	t.Parallel()
	s := NewStore()
	s.Set("key", []byte("value"))
	val, ok := s.Get("key")
	require.True(t, ok)
	assert.Equal(t, []byte("value"), val)
}

func TestGetMissing(t *testing.T) {
	t.Parallel()
	s := NewStore()
	_, ok := s.Get("missing")
	assert.False(t, ok)
}

func TestDelete(t *testing.T) {
	t.Parallel()
	s := NewStore()
	s.Set("key", []byte("value"))
	deleted := s.Delete("key")
	assert.True(t, deleted)
	_, ok := s.Get("key")
	assert.False(t, ok)
}

func TestDeleteMissing(t *testing.T) {
	t.Parallel()
	s := NewStore()
	deleted := s.Delete("nonexistent")
	assert.False(t, deleted)
}

func TestTTLExpiry(t *testing.T) {
	t.Parallel()
	s := NewStore()
	s.SetWithTTL("key", []byte("value"), 50*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	_, ok := s.Get("key")
	assert.False(t, ok, "key should have expired")
}

func TestTTLNotExpired(t *testing.T) {
	t.Parallel()
	s := NewStore()
	s.SetWithTTL("key", []byte("value"), 1*time.Second)
	val, ok := s.Get("key")
	require.True(t, ok, "key should still be present")
	assert.Equal(t, []byte("value"), val)
}

func TestOverwrite(t *testing.T) {
	t.Parallel()
	s := NewStore()
	s.Set("key", []byte("first"))
	s.Set("key", []byte("second"))
	val, ok := s.Get("key")
	require.True(t, ok)
	assert.Equal(t, []byte("second"), val)
}

func TestSetClearsTTL(t *testing.T) {
	t.Parallel()
	s := NewStore()
	s.SetWithTTL("key", []byte("ttl-value"), 50*time.Millisecond)
	s.Set("key", []byte("permanent"))
	time.Sleep(100 * time.Millisecond)
	val, ok := s.Get("key")
	require.True(t, ok, "key should survive past original TTL after Set overwrite")
	assert.Equal(t, []byte("permanent"), val)
}

func TestConcurrentReads(t *testing.T) {
	t.Parallel()
	s := NewStore()
	s.Set("shared", []byte("data"))

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			val, ok := s.Get("shared")
			assert.True(t, ok)
			assert.Equal(t, []byte("data"), val)
		}()
	}
	wg.Wait()
}

func TestConcurrentWrites(t *testing.T) {
	t.Parallel()
	s := NewStore()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i)
			s.Set(key, []byte(key))
		}()
	}
	wg.Wait()

	assert.Equal(t, 50, s.Len())
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key-%d", i)
		val, ok := s.Get(key)
		assert.True(t, ok, "key %s should exist", key)
		assert.Equal(t, []byte(key), val)
	}
}

func TestExpiryLoop(t *testing.T) {
	t.Parallel()
	s := NewStore()
	for i := 0; i < 5; i++ {
		s.SetWithTTL(fmt.Sprintf("key-%d", i), []byte("v"), 50*time.Millisecond)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.StartExpiryLoop(ctx, 25*time.Millisecond)

	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, 0, s.Len())
}
