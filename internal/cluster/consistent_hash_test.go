package cluster

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestAddNode(t *testing.T) {
	ring := NewHashRing(150)
	ring.AddNode("node1")
	ring.AddNode("node2")
	ring.AddNode("node3")

	if ring.Len() != 3 {
		t.Fatalf("expected 3 nodes, got %d", ring.Len())
	}

	valid := map[NodeID]bool{"node1": true, "node2": true, "node3": true}
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		id, ok := ring.GetNode(key)
		if !ok {
			t.Fatalf("GetNode returned false for key %q", key)
		}
		if !valid[id] {
			t.Fatalf("GetNode returned unexpected node %q", id)
		}
	}
}

func TestRemoveNode(t *testing.T) {
	ring := NewHashRing(150)
	ring.AddNode("node1")
	ring.AddNode("node2")
	ring.AddNode("node3")
	ring.RemoveNode("node2")

	if ring.Len() != 2 {
		t.Fatalf("expected 2 nodes after removal, got %d", ring.Len())
	}

	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key-%d", i)
		id, ok := ring.GetNode(key)
		if !ok {
			t.Fatalf("GetNode returned false for key %q", key)
		}
		if id == "node2" {
			t.Fatalf("removed node2 still returned for key %q", key)
		}
	}
}

func TestDistribution(t *testing.T) {
	ring := NewHashRing(150)
	ring.AddNode("node1")
	ring.AddNode("node2")
	ring.AddNode("node3")

	counts := map[NodeID]int{}
	total := 10000
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("key-%d", i)
		id, _ := ring.GetNode(key)
		counts[id]++
	}

	for id, count := range counts {
		pct := float64(count) / float64(total)
		t.Logf("node %s: %d keys (%.1f%%)", id, count, pct*100)
		if pct < 0.25 || pct > 0.42 {
			t.Errorf("node %s has %.1f%% of keys — outside [25%%, 42%%]", id, pct*100)
		}
	}
}

func TestConsistency(t *testing.T) {
	ring := NewHashRing(150)
	ring.AddNode("node1")
	ring.AddNode("node2")
	ring.AddNode("node3")

	total := 1000
	before := make(map[string]NodeID, total)
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("key-%d", i)
		id, _ := ring.GetNode(key)
		before[key] = id
	}

	ring.AddNode("node4")

	moved := 0
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("key-%d", i)
		id, _ := ring.GetNode(key)
		if id != before[key] {
			moved++
		}
	}

	pct := float64(moved) / float64(total) * 100
	t.Logf("Keys moved after adding node: %d/%d (%.1f%%)", moved, total, pct)
	if pct >= 35 {
		t.Errorf("too many keys moved: %.1f%% >= 35%%", pct)
	}
}

func TestWrapAround(t *testing.T) {
	ring := NewHashRing(150)
	ring.AddNode("only-node")

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		id, ok := ring.GetNode(key)
		if !ok {
			t.Fatalf("GetNode returned false for key %q", key)
		}
		if id != "only-node" {
			t.Fatalf("expected only-node, got %q for key %q", id, key)
		}
	}
}

func TestGetNodes(t *testing.T) {
	ring := NewHashRing(150)
	ring.AddNode("node1")
	ring.AddNode("node2")
	ring.AddNode("node3")

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		nodes := ring.GetNodes(key, 3)
		if len(nodes) != 3 {
			t.Fatalf("expected 3 nodes, got %d for key %q", len(nodes), key)
		}
		seen := map[NodeID]bool{}
		for _, id := range nodes {
			if seen[id] {
				t.Fatalf("duplicate node %q in GetNodes result for key %q", id, key)
			}
			seen[id] = true
		}
	}
}

func TestConcurrentAccess(t *testing.T) {
	ring := NewHashRing(150)
	for i := 1; i <= 5; i++ {
		ring.AddNode(NodeID(fmt.Sprintf("node%d", i)))
	}

	deadline := time.Now().Add(time.Second)
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for time.Now().Before(deadline) {
				key := fmt.Sprintf("key-%d", n)
				ring.GetNode(key)
			}
		}(i)
	}

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for time.Now().Before(deadline) {
				id := NodeID(fmt.Sprintf("dynamic%d", n))
				ring.AddNode(id)
				ring.RemoveNode(id)
			}
		}(i)
	}

	wg.Wait()
}
