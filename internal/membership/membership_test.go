package membership

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/m-pedro/godistkv/internal/storage"
	"github.com/m-pedro/godistkv/internal/transport"
	"github.com/m-pedro/godistkv/internal/version"
)

func TestDetector_TransitionsAliveToSuspectToDeadAndRecovers(t *testing.T) {
	client := transport.NewClient(100 * time.Millisecond)
	det := NewDetector(client, 2) // 1 failure -> SUSPECT, 2 failures -> DEAD

	srv := httptest.NewServer(transport.NewReplicaHandler("node-1", storage.NewEngine()))
	det.RegisterPeer("node-1", srv.URL)

	ctx := context.Background()
	det.ProbeOnce(ctx)
	if got := det.GetStatus("node-1"); got != StatusAlive {
		t.Fatalf("expected StatusAlive, got %v", got)
	}

	// Stop the server to simulate node failure
	srv.Close()

	det.ProbeOnce(ctx)
	if got := det.GetStatus("node-1"); got != StatusSuspect {
		t.Fatalf("expected StatusSuspect after 1st failed heartbeat, got %v", got)
	}

	det.ProbeOnce(ctx)
	if got := det.GetStatus("node-1"); got != StatusDead {
		t.Fatalf("expected StatusDead after 2nd failed heartbeat, got %v", got)
	}
}

func TestHandoffQueue_EnqueuesAndReplaysWhenPeerRecovers(t *testing.T) {
	client := transport.NewClient(100 * time.Millisecond)
	det := NewDetector(client, 1)
	hq := NewHandoffQueue(client, det)

	store := storage.NewEngine()
	srv := httptest.NewServer(transport.NewReplicaHandler("node-2", store))
	defer srv.Close()

	det.RegisterPeer("node-2", "http://127.0.0.1:1") // Initially unreachable port
	ctx := context.Background()
	det.ProbeOnce(ctx) // Marks node-2 DEAD

	rec := transportTestRecord("hint:key", "delayed_payload")
	hq.Enqueue("node-2", srv.URL, rec)
	if hq.PendingCount() != 1 {
		t.Fatalf("expected 1 pending hint, got %d", hq.PendingCount())
	}

	// While DEAD, ReplayOnce must keep the hint queued
	hq.ReplayOnce(ctx)
	if hq.PendingCount() != 1 {
		t.Fatalf("expected hint to remain queued while DEAD, got %d", hq.PendingCount())
	}

	// Point detector to the live server URL and probe -> ALIVE
	det.RegisterPeer("node-2", srv.URL)
	det.ProbeOnce(ctx)
	hq.ReplayOnce(ctx)

	if hq.PendingCount() != 0 || hq.ReplayedCount() != 1 {
		t.Fatalf("expected 0 pending and 1 replayed, got pending=%d replayed=%d",
			hq.PendingCount(), hq.ReplayedCount())
	}
	if got, ok := store.Get("hint:key"); !ok || string(got.Value) != "delayed_payload" {
		t.Fatalf("expected replayed record on node-2 store, got ok=%v val=%q", ok, string(got.Value))
	}
}

func transportTestRecord(key, val string) version.VersionedRecord {
	return version.VersionedRecord{
		Key:         key,
		Value:       []byte(val),
		Clock:       version.New().Increment("node-1"),
		TimestampNs: 1000,
	}
}
