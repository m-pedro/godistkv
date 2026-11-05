package version

import "testing"

func TestVectorClock_CausalOrderingAndConcurrentLWW(t *testing.T) {
	c1 := New().Increment("node-a")
	c2 := c1.Increment("node-b")

	if c1.Compare(c2) != Before {
		t.Fatalf("expected c1 Before c2, got %v", c1.Compare(c2))
	}
	if c2.Compare(c1) != After {
		t.Fatalf("expected c2 After c1, got %v", c2.Compare(c1))
	}

	// Create concurrent sibling branches from c1
	branchA := c1.Increment("node-a")
	branchB := c1.Increment("node-c")

	if branchA.Compare(branchB) != Concurrent {
		t.Fatalf("expected sibling branches to be Concurrent, got %v", branchA.Compare(branchB))
	}

	recA := VersionedRecord{
		Key:         "cart:1",
		Value:       []byte("item_older"),
		Clock:       branchA,
		TimestampNs: 1000,
	}
	recB := VersionedRecord{
		Key:         "cart:1",
		Value:       []byte("item_newer"),
		Clock:       branchB,
		TimestampNs: 2000,
	}

	resolved := ResolveWinner(recA, recB)
	if string(resolved.Value) != "item_newer" {
		t.Fatalf("expected LWW winner 'item_newer', got %q", string(resolved.Value))
	}
	if resolved.Clock["node-a"] != 2 || resolved.Clock["node-c"] != 1 {
		t.Fatalf("expected merged vector clock {node-a:2, node-c:1}, got %v", resolved.Clock)
	}
}
