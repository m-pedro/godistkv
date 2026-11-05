package transport

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/m-pedro/godistkv/internal/storage"
	"github.com/m-pedro/godistkv/internal/version"
)

func TestTransport_PutGetAndPingRoundtrip(t *testing.T) {
	store := storage.NewEngine()
	handler := NewReplicaHandler("node-1", store)
	srv := httptest.NewServer(handler)
	defer srv.Close()

	client := NewClient(200 * time.Millisecond)
	ctx := context.Background()

	if err := client.Ping(ctx, srv.URL); err != nil {
		t.Fatalf("expected ping to succeed: %v", err)
	}

	// Missing key returns found=false without error
	_, found, err := client.GetReplica(ctx, srv.URL, "missing")
	if err != nil || found {
		t.Fatalf("expected not found, got found=%v err=%v", found, err)
	}

	rec := version.VersionedRecord{
		Key:         "order:77",
		Value:       []byte("confirmed"),
		Clock:       version.New().Increment("node-1"),
		TimestampNs: 123456,
	}
	stored, err := client.PutReplica(ctx, srv.URL, rec)
	if err != nil {
		t.Fatalf("PutReplica failed: %v", err)
	}
	if string(stored.Value) != "confirmed" {
		t.Fatalf("unexpected stored value: %s", string(stored.Value))
	}

	got, found, err := client.GetReplica(ctx, srv.URL, "order:77")
	if err != nil || !found {
		t.Fatalf("expected found=true, err=nil; got found=%v err=%v", found, err)
	}
	if string(got.Value) != "confirmed" || got.Clock["node-1"] != 1 {
		t.Fatalf("unexpected fetched record: %+v", got)
	}
}
