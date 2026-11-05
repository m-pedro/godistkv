package storage

import (
	"fmt"
	"sync"
	"testing"

	"github.com/m-pedro/godistkv/internal/version"
)

func TestEngine_RejectsCausallyStaleWrite(t *testing.T) {
	eng := NewEngine()

	c1 := version.New().Increment("node-1")
	c2 := c1.Increment("node-1")

	newer := version.VersionedRecord{
		Key:         "profile:10",
		Value:       []byte("v2_latest"),
		Clock:       c2,
		TimestampNs: 200,
	}
	older := version.VersionedRecord{
		Key:         "profile:10",
		Value:       []byte("v1_stale"),
		Clock:       c1,
		TimestampNs: 500, // Even with higher wall-clock, causal VectorClock c2 > c1 must win!
	}

	if _, applied := eng.Put(newer); !applied {
		t.Fatal("expected newer write to be applied")
	}
	winner, applied := eng.Put(older)
	if applied {
		t.Fatal("expected causally stale write (c1 < c2) to be rejected")
	}
	if string(winner.Value) != "v2_latest" {
		t.Fatalf("expected 'v2_latest' to remain stored, got %q", string(winner.Value))
	}
}

func TestEngine_ConcurrentAccessWithRaceDetector(t *testing.T) {
	eng := NewEngine()
	var wg sync.WaitGroup

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				key := fmt.Sprintf("key:%d", j%10)
				vc := version.New().Increment(fmt.Sprintf("node-%d", workerID))
				eng.Put(version.VersionedRecord{
					Key:         key,
					Value:       []byte("payload"),
					Clock:       vc,
					TimestampNs: int64(j),
				})
				_, _ = eng.Get(key)
			}
		}(i)
	}
	wg.Wait()

	if eng.Len() != 10 {
		t.Fatalf("expected 10 keys stored, got %d", eng.Len())
	}
}
