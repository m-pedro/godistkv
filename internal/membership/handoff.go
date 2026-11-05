package membership

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m-pedro/godistkv/internal/transport"
	"github.com/m-pedro/godistkv/internal/version"
)

// Hint represents a write destined for a temporarily unreachable peer replica.
type Hint struct {
	TargetNodeID string
	TargetURL    string
	Record       version.VersionedRecord
}

// HandoffQueue stores pending hints per target node and replays them once the node recovers.
type HandoffQueue struct {
	mu       sync.Mutex
	queues   map[string][]Hint // targetNodeID -> pending hints
	client   *transport.Client
	detector *Detector
	replayed atomic.Uint64
}

func NewHandoffQueue(client *transport.Client, detector *Detector) *HandoffQueue {
	return &HandoffQueue{
		queues:   make(map[string][]Hint),
		client:   client,
		detector: detector,
	}
}

// Enqueue records a hint for `targetNodeID` when a direct replica write fails.
func (hq *HandoffQueue) Enqueue(targetNodeID, targetURL string, rec version.VersionedRecord) {
	hq.mu.Lock()
	defer hq.mu.Unlock()

	hq.queues[targetNodeID] = append(hq.queues[targetNodeID], Hint{
		TargetNodeID: targetNodeID,
		TargetURL:    targetURL,
		Record:       rec,
	})
}

// PendingCount returns the total number of hints waiting across all target nodes.
func (hq *HandoffQueue) PendingCount() int {
	hq.mu.Lock()
	defer hq.mu.Unlock()

	total := 0
	for _, list := range hq.queues {
		total += len(list)
	}
	return total
}

func (hq *HandoffQueue) ReplayedCount() uint64 {
	return hq.replayed.Load()
}

// ReplayOnce attempts to deliver queued hints to any target node that is Alive (or reachable).
func (hq *HandoffQueue) ReplayOnce(ctx context.Context) {
	hq.mu.Lock()
	snapshot := make(map[string][]Hint, len(hq.queues))
	for nodeID, list := range hq.queues {
		if len(list) > 0 {
			copied := make([]Hint, len(list))
			copy(copied, list)
			snapshot[nodeID] = copied
			delete(hq.queues, nodeID)
		}
	}
	hq.mu.Unlock()

	for nodeID, hints := range snapshot {
		if hq.detector != nil && !hq.detector.IsAlive(nodeID) {
			// Node is still marked DEAD; re-queue hints
			hq.requeue(nodeID, hints)
			continue
		}

		var failed []Hint
		for _, h := range hints {
			if _, err := hq.client.PutReplica(ctx, h.TargetURL, h.Record); err != nil {
				failed = append(failed, h)
			} else {
				hq.replayed.Add(1)
			}
		}
		if len(failed) > 0 {
			hq.requeue(nodeID, failed)
		}
	}
}

func (hq *HandoffQueue) requeue(nodeID string, failed []Hint) {
	hq.mu.Lock()
	defer hq.mu.Unlock()
	hq.queues[nodeID] = append(failed, hq.queues[nodeID]...)
}

// Start runs a background loop replaying hints at `interval` until `ctx` is canceled.
func (hq *HandoffQueue) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			hq.ReplayOnce(ctx)
		}
	}
}
