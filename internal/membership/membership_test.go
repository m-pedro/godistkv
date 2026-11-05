package membership

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/m-pedro/godistkv/internal/storage"
	"github.com/m-pedro/godistkv/internal/transport"
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
