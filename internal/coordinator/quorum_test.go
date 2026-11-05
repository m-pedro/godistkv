package coordinator

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/m-pedro/godistkv/internal/ring"
	"github.com/m-pedro/godistkv/internal/storage"
	"github.com/m-pedro/godistkv/internal/transport"
)

func TestCoordinator_QuorumSurvivesSingleNodeOutage(t *testing.T) {
	r, _ := ring.New(64)
	client := transport.NewClient(200 * time.Millisecond)
	coord := New(Config{
		NodeID:  "node-1",
		N:       3,
		R:       2,
		W:       2,
		Timeout: 300 * time.Millisecond,
	}, r, client)

	stores := make([]*storage.Engine, 3)
	servers := make([]*httptest.Server, 3)
	for i := 0; i < 3; i++ {
		nodeID := []string{"node-1", "node-2", "node-3"}[i]
		stores[i] = storage.NewEngine()
		servers[i] = httptest.NewServer(transport.NewReplicaHandler(nodeID, stores[i]))
		coord.RegisterPeer(nodeID, servers[i].URL)
	}
	defer func() {
		for _, s := range servers {
			s.Close()
		}
	}()

	// Shut down node-3 to simulate a 1-node crash in a 3-node cluster (N=3, R=2, W=2)
	servers[2].Close()

	ctx := context.Background()
	if _, err := coord.Put(ctx, "session:abc", []byte("alive_quorum")); err != nil {
		t.Fatalf("expected W=2 write quorum to succeed with 2/3 live nodes, got: %v", err)
	}

	got, found, err := coord.Get(ctx, "session:abc")
	if err != nil || !found {
		t.Fatalf("expected R=2 read quorum to succeed, got found=%v err=%v", found, err)
	}
	if string(got.Value) != "alive_quorum" {
		t.Fatalf("unexpected value: %q", string(got.Value))
	}

	// Shut down node-2 as well (only 1/3 nodes alive -> W=2 and R=2 must fail)
	servers[1].Close()
	if _, err := coord.Put(ctx, "session:abc", []byte("should_fail")); err == nil {
		t.Fatal("expected W=2 write to fail when only 1/3 nodes are reachable")
	}
}

func TestCoordinator_AsyncReadRepairHealsStaleReplica(t *testing.T) {
	r, _ := ring.New(64)
	client := transport.NewClient(200 * time.Millisecond)
	coord := New(Config{
		NodeID:  "node-1",
		N:       3,
		R:       2,
		W:       2,
		Timeout: 300 * time.Millisecond,
	}, r, client)

	stores := make([]*storage.Engine, 3)
	servers := make([]*httptest.Server, 3)
	for i := 0; i < 3; i++ {
		nodeID := []string{"node-1", "node-2", "node-3"}[i]
		stores[i] = storage.NewEngine()
		servers[i] = httptest.NewServer(transport.NewReplicaHandler(nodeID, stores[i]))
		coord.RegisterPeer(nodeID, servers[i].URL)
	}
	defer func() {
		for _, s := range servers {
			s.Close()
		}
	}()

	// Write directly to node-1 and node-2 only, leaving node-3 completely empty (simulating a missed write)
	rec, err := coord.Put(context.Background(), "repair_key", []byte("healed_value"))
	if err != nil {
		t.Fatalf("unexpected put error: %v", err)
	}
	// Manually wipe node-3's store to simulate a replica that missed the write
	*stores[2] = *storage.NewEngine()
	if _, ok := stores[2].Get("repair_key"); ok {
		t.Fatal("expected node-3 to be missing repair_key before read repair")
	}

	// Perform a Quorum Get: node-3 responds with 404, triggering async Read Repair
	got, found, err := coord.Get(context.Background(), "repair_key")
	if err != nil || !found || string(got.Value) != string(rec.Value) {
		t.Fatalf("unexpected Get result: found=%v err=%v", found, err)
	}

	// Wait briefly for the background Read Repair goroutine to heal node-3
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if repaired, ok := stores[2].Get("repair_key"); ok && string(repaired.Value) == "healed_value" {
			return // Read repair succeeded!
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for async Read Repair to heal stale replica node-3")
}
