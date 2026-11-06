package internal

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/m-pedro/godistkv/internal/coordinator"
	"github.com/m-pedro/godistkv/internal/ring"
	"github.com/m-pedro/godistkv/internal/storage"
	"github.com/m-pedro/godistkv/internal/transport"
	"github.com/m-pedro/godistkv/internal/version"
)

func BenchmarkRing_GetPreferenceList(b *testing.B) {
	r, _ := ring.New(128)
	for i := 1; i <= 10; i++ {
		r.AddNode(fmt.Sprintf("node-%d", i))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = r.GetPreferenceList("user:session:987654321", 3)
	}
}

func BenchmarkVectorClock_CompareAndMerge(b *testing.B) {
	vcA := version.New().Increment("node-1").Increment("node-2")
	vcB := version.New().Increment("node-2").Increment("node-3")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = vcA.Compare(vcB)
		_ = vcA.Merge(vcB)
	}
}

func BenchmarkStorageEngine_ParallelPutGet(b *testing.B) {
	eng := storage.NewEngine()
	vc := version.New().Increment("node-1")
	rec := version.VersionedRecord{
		Key:         "hot:key",
		Value:       []byte("benchmark_payload_value"),
		Clock:       vc,
		TimestampNs: 1000,
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			eng.Put(rec)
			_, _ = eng.Get("hot:key")
		}
	})
}

func BenchmarkCoordinator_QuorumPutAndGet(b *testing.B) {
	r, _ := ring.New(64)
	client := transport.NewClient(500 * time.Millisecond)
	coord := coordinator.New(coordinator.Config{
		NodeID:  "node-1",
		N:       3,
		R:       2,
		W:       2,
		Timeout: 500 * time.Millisecond,
	}, r, client)

	var servers []*httptest.Server
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("node-%d", i)
		srv := httptest.NewServer(transport.NewReplicaHandler(id, storage.NewEngine()))
		servers = append(servers, srv)
		coord.RegisterPeer(id, srv.URL)
	}
	defer func() {
		for _, s := range servers {
			s.Close()
		}
	}()

	ctx := context.Background()
	payload := []byte("quorum_value_123")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = coord.Put(ctx, "bench:key", payload)
		_, _, _ = coord.Get(ctx, "bench:key")
	}
}
