package internal

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m-pedro/godistkv/internal/coordinator"
	"github.com/m-pedro/godistkv/internal/membership"
	"github.com/m-pedro/godistkv/internal/ring"
	"github.com/m-pedro/godistkv/internal/storage"
	"github.com/m-pedro/godistkv/internal/transport"
)

// faultSwitchHandler wraps an http.Handler with an atomic kill switch to simulate network partitions.
type faultSwitchHandler struct {
	partitioned atomic.Bool
	inner       http.Handler
}

func (f *faultSwitchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if f.partitioned.Load() {
		http.Error(w, "simulated network partition", http.StatusServiceUnavailable)
		return
	}
	f.inner.ServeHTTP(w, r)
}

func TestClusterChaos_NodePartitionHintedHandoffAndDeleteReconciliation(t *testing.T) {
	r, _ := ring.New(64)
	client := transport.NewClient(150 * time.Millisecond)
	det := membership.NewDetector(client, 1)
	handoff := membership.NewHandoffQueue(client, det)

	coord := coordinator.New(coordinator.Config{
		NodeID:  "node-1",
		N:       3,
		R:       2,
		W:       2,
		Timeout: 250 * time.Millisecond,
	}, r, client)
	coord.SetHintSink(handoff)

	nodeIDs := []string{"node-1", "node-2", "node-3"}
	stores := make([]*storage.Engine, 3)
	switches := make([]*faultSwitchHandler, 3)
	servers := make([]*httptest.Server, 3)

	for i, id := range nodeIDs {
		stores[i] = storage.NewEngine()
		switches[i] = &faultSwitchHandler{
			inner: transport.NewReplicaHandler(id, stores[i]),
		}
		servers[i] = httptest.NewServer(switches[i])
		coord.RegisterPeer(id, servers[i].URL)
		det.RegisterPeer(id, servers[i].URL)
	}
	defer func() {
		for _, s := range servers {
			s.Close()
		}
	}()

	ctx := context.Background()

	// 1. Partition node-3 so it rejects all RPCs with 503
	switches[2].partitioned.Store(true)
	det.ProbeOnce(ctx)

	// 2. Write 5 keys with W=2: node-1 and node-2 ack, while node-3's missed writes are queued in HandoffQueue
	for i := 0; i < 5; i++ {
		key := "acct:" + string(rune('a'+i))
		if _, err := coord.Put(ctx, key, []byte("balance_500")); err != nil {
			t.Fatalf("expected W=2 write to succeed during single-node partition: %v", err)
		}
	}

	// Wait briefly for the failed node-3 RPC goroutines to enqueue their hints
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) && handoff.PendingCount() < 5 {
		time.Sleep(10 * time.Millisecond)
	}
	if handoff.PendingCount() != 5 {
		t.Fatalf("expected 5 hints queued for partitioned node-3, got %d", handoff.PendingCount())
	}
	if stores[2].Len() != 0 {
		t.Fatalf("expected partitioned node-3 store to have 0 keys, got %d", stores[2].Len())
	}

	// 3. Heal node-3's network partition, probe liveness, and replay hinted handoff queue
	switches[2].partitioned.Store(false)
	det.ProbeOnce(ctx)
	handoff.ReplayOnce(ctx)

	if handoff.PendingCount() != 0 {
		t.Fatalf("expected 0 pending hints after replay, got %d", handoff.PendingCount())
	}
	if stores[2].Len() != 5 {
		t.Fatalf("expected node-3 to hold all 5 replayed keys after healing, got %d", stores[2].Len())
	}

	// 4. Verify tombstone Delete propagates across quorum and hides deleted key
	if _, err := coord.Delete(ctx, "acct:a"); err != nil {
		t.Fatalf("unexpected delete error: %v", err)
	}
	if _, found, err := coord.Get(ctx, "acct:a"); err != nil || found {
		t.Fatalf("expected deleted key acct:a to return found=false, got found=%v err=%v", found, err)
	}
}
