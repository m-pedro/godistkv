package coordinator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m-pedro/godistkv/internal/ring"
	"github.com/m-pedro/godistkv/internal/transport"
	"github.com/m-pedro/godistkv/internal/version"
)

var (
	ErrWriteQuorumFailed = errors.New("write quorum (W) not reached")
	ErrReadQuorumFailed  = errors.New("read quorum (R) not reached")
)

// HintSink records failed replica writes for asynchronous Hinted Handoff replay.
type HintSink interface {
	Enqueue(targetNodeID, targetURL string, rec version.VersionedRecord)
}

type Config struct {
	NodeID  string
	N       int
	R       int
	W       int
	Timeout time.Duration
}

type Coordinator struct {
	cfg      Config
	ring     *ring.Ring
	client   *transport.Client
	hintSink HintSink

	mu          sync.RWMutex
	peers       map[string]string
	readRepairs atomic.Uint64
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

func (c *Coordinator) SetHintSink(sink HintSink) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hintSink = sink
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

func (c *Coordinator) ReadRepairsCompleted() uint64 {
	return c.readRepairs.Load()
}

func (c *Coordinator) Put(ctx context.Context, key string, value []byte) (version.VersionedRecord, error) {
	return c.writeRecord(ctx, key, value, false)
}

func (c *Coordinator) Delete(ctx context.Context, key string) (version.VersionedRecord, error) {
	return c.writeRecord(ctx, key, nil, true)
}

func (c *Coordinator) writeRecord(ctx context.Context, key string, value []byte, tombstone bool) (version.VersionedRecord, error) {
	replicas, err := c.ring.GetPreferenceList(key, c.cfg.N)
	if err != nil {
		return version.VersionedRecord{}, err
	}

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
		url    string
		err    error
	}
	resCh := make(chan writeResult, len(replicas))

	c.mu.RLock()
	sink := c.hintSink
	c.mu.RUnlock()

	for _, nodeID := range replicas {
		url, ok := c.PeerURL(nodeID)
		if !ok {
			resCh <- writeResult{nodeID: nodeID, err: fmt.Errorf("unknown peer %s", nodeID)}
			continue
		}
		go func(id, peerURL string) {
			_, putErr := c.client.PutReplica(reqCtx, peerURL, rec)
			if putErr != nil && sink != nil {
				sink.Enqueue(id, peerURL, rec)
			}
			resCh <- writeResult{nodeID: id, url: peerURL, err: putErr}
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

type replicaReadResult struct {
	nodeID string
	url    string
	rec    version.VersionedRecord
	found  bool
	err    error
}

func (c *Coordinator) Get(ctx context.Context, key string) (version.VersionedRecord, bool, error) {
	replicas, err := c.ring.GetPreferenceList(key, c.cfg.N)
	if err != nil {
		return version.VersionedRecord{}, false, err
	}

	reqCtx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()

	resCh := make(chan replicaReadResult, len(replicas))
	for _, nodeID := range replicas {
		url, ok := c.PeerURL(nodeID)
		if !ok {
			resCh <- replicaReadResult{nodeID: nodeID, err: fmt.Errorf("unknown peer %s", nodeID)}
			continue
		}
		go func(id, peerURL string) {
			rec, found, getErr := c.client.GetReplica(reqCtx, peerURL, key)
			resCh <- replicaReadResult{
				nodeID: id,
				url:    peerURL,
				rec:    rec,
				found:  found,
				err:    getErr,
			}
		}(nodeID, url)
	}

	var (
		successResponses []replicaReadResult
		haveWinner       bool
		winner           version.VersionedRecord
	)

	for i := 0; i < len(replicas); i++ {
		res := <-resCh
		if res.err != nil {
			continue
		}
		successResponses = append(successResponses, res)
		if res.found {
			if !haveWinner {
				winner = res.rec
				haveWinner = true
			} else {
				winner = version.ResolveWinner(winner, res.rec)
			}
		}
	}

	if len(successResponses) < c.cfg.R {
		return version.VersionedRecord{}, false, fmt.Errorf(
			"%w: got %d/%d responses",
			ErrReadQuorumFailed, len(successResponses), c.cfg.R,
		)
	}

	if !haveWinner {
		return version.VersionedRecord{}, false, nil
	}

	c.triggerReadRepair(winner, successResponses)

	if winner.Tombstone {
		return version.VersionedRecord{}, false, nil
	}
	return winner, true, nil
}

func (c *Coordinator) triggerReadRepair(winner version.VersionedRecord, responses []replicaReadResult) {
	for _, res := range responses {
		needsRepair := !res.found || res.rec.Clock.Compare(winner.Clock) != version.Equal
		if needsRepair && res.url != "" {
			peerURL := res.url
			go func() {
				repairCtx, cancel := context.WithTimeout(context.Background(), c.cfg.Timeout)
				defer cancel()
				if _, err := c.client.PutReplica(repairCtx, peerURL, winner); err == nil {
					c.readRepairs.Add(1)
				}
			}()
		}
	}
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
