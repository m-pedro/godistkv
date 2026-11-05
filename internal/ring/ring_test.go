package ring

import (
	"fmt"
	"math"
	"testing"
)

func TestPreferenceList_DistinctPhysicalNodes(t *testing.T) {
	r, err := New(64)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r.AddNode("node-a")
	r.AddNode("node-b")
	r.AddNode("node-c")
	r.AddNode("node-d")

	replicas, err := r.GetPreferenceList("user:9001", 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(replicas) != 3 {
		t.Fatalf("expected 3 replicas, got %d", len(replicas))
	}

	seen := map[string]bool{}
	for _, n := range replicas {
		if seen[n] {
			t.Fatalf("duplicate physical node %s in preference list %v", n, replicas)
		}
		seen[n] = true
	}
}

func TestConsistentHash_MinimalRemappingOnNodeAddition(t *testing.T) {
	r, _ := New(128)
	nodes := []string{"node-1", "node-2", "node-3", "node-4"}
	for _, n := range nodes {
		r.AddNode(n)
	}

	const totalKeys = 10_000
	beforeOwner := make([]string, totalKeys)
	counts := make(map[string]int)

	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("key:%d", i)
		pref, _ := r.GetPreferenceList(key, 1)
		beforeOwner[i] = pref[0]
		counts[pref[0]]++
	}

	// Verify load balance across 4 nodes (ideal = 2,500 keys per node)
	ideal := float64(totalKeys) / float64(len(nodes))
	for n, c := range counts {
		deviation := math.Abs(float64(c)-ideal) / ideal
		if deviation > 0.20 {
			t.Errorf("node %s has %d keys (deviation %.1f%% > 20%%)", n, c, deviation*100)
		}
	}

	// Add a 5th node ("node-5"): theoretical remapping fraction is ~1/5 (20%)
	r.AddNode("node-5")
	moved := 0
	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("key:%d", i)
		pref, _ := r.GetPreferenceList(key, 1)
		if pref[0] != beforeOwner[i] {
			moved++
		}
	}

	movedRatio := float64(moved) / float64(totalKeys)
	if movedRatio < 0.12 || movedRatio > 0.28 {
		t.Fatalf("expected ~20%% of keys to remap when adding 5th node, got %.2f%% (%d/%d)",
			movedRatio*100, moved, totalKeys)
	}
}
