package coordinator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/m-pedro/godistkv/internal/ring"
	"github.com/m-pedro/godistkv/internal/transport"
	"github.com/m-pedro/godistkv/internal/version"
)

var (
	ErrWriteQuorumFailed = errors.New("write quorum (W) not reached")
	ErrReadQuorumFailed  = errors.New("read quorum (R) not reached")
)

type Config struct {
	NodeID    string
	N         int
	R         int
	W         int
	Timeout   time.Duration
}

type Coordinator struct {
	cfg    Config
	ring   *ring.Ring
	client *transport.Client

	mu    sync.RWMutex
	peers map[string]string // nodeID -> base HTTP URL (e.g. "http://127.0.0.1:8001")
}

func New(cfg Config, r *ring.Ring, client *transport.Client) *Coordinator {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 500 * time.Millisecond
	}
	return &Coordinator{
		cfg:    cfg,
		ring:   r,
		client: client,
		peers:  make(map[string]string),
	}
}

func (c *Coordinator) RegisterPeer(nodeID, baseURL string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.peers[nodeID] = baseURL
	c.ring.AddNode(nodeID)
}

func (c *Coordinator) PeerURL(nodeID string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	u, ok := c.peers[nodeID]
	return u, ok
}

// Put coordinates a scatter-gather Dynamo write across N preference-list replicas
// and succeeds as soon as W replicas acknowledge.
func (c *Coordinator) Put(ctx context.Context, key string, value []byte) (version.VersionedRecord, error) {
	return c.writeRecord(ctx, key, value, false)
}

// Delete coordinates a tombstone write across N preference-list replicas with W quorum.
func (c *Coordinator) Delete(ctx context.Context, key string) (version.VersionedRecord, error) {
	return c.writeRecord(ctx, key, nil, true)
}

func (c *Coordinator) writeRecord(ctx context.Context, key string, value []byte, tombstone bool) (version.VersionedRecord, error) {
	replicas, err := c.ring.GetPreferenceList(key, c.cfg.N)
	if err != nil {
		return version.VersionedRecord{}, err
	}

	// Read existing highest clock from coordinator's first reachable replica (or start fresh)
	baseClock := c.fetchBaseClock(ctx, key, replicas)
	nextClock := baseClock.Increment(c.cfg.NodeID)

	rec := version.VersionedRecord{
		Key:         key,
		Value:       value,
		Tombstone:   tombstone,
		Clock:       nextClock,
		TimestampNs: time.Now().UnixNano(),
	}

	reqCtx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()

	type writeResult struct {
		nodeID string
		err    error
	}
	resCh := make(chan writeResult, len(replicas))

	for _, nodeID := range replicas {
		url, ok := c.PeerURL(nodeID)
		if !ok {
			resCh <- writeResult{nodeID: nodeID, err: fmt.Errorf("unknown peer %s", nodeID)}
			continue
		}
		go func(id, peerURL string) {
			_, putErr := c.client.PutReplica(reqCtx, peerURL, rec)
			resCh <- writeResult{nodeID: id, err: putErr}
		}(nodeID, url)
	}

	acks := 0
	for i := 0; i < len(replicas); i++ {
		res := <-resCh
		if res.err == nil {
			acks++
			if acks >= c.cfg.W {
				return rec, nil
			}
		}
	}

	return version.VersionedRecord{}, fmt.Errorf("%w: got %d/%d acks", ErrWriteQuorumFailed, acks, c.cfg.W)
}

func (c *Coordinator) fetchBaseClock(ctx context.Context, key string, replicas []string) version.VectorClock {
	probeCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	merged := version.New()
	for _, nodeID := range replicas {
		if url, ok := c.PeerURL(nodeID); ok {
			if rec, found, err := c.client.GetReplica(probeCtx, url, key); err == nil && found {
				merged = merged.Merge(rec.Clock)
			}
		}
	}
	return merged
}
