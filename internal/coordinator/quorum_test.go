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
