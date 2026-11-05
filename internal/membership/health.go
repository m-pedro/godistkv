package membership

import (
	"context"
	"sync"
	"time"

	"github.com/m-pedro/godistkv/internal/transport"
)

type Status int

const (
	StatusAlive Status = iota
	StatusSuspect
	StatusDead
)

func (s Status) String() string {
	switch s {
	case StatusAlive:
		return "ALIVE"
	case StatusSuspect:
		return "SUSPECT"
	case StatusDead:
		return "DEAD"
	default:
		return "UNKNOWN"
	}
}

type PeerState struct {
	NodeID              string
	BaseURL             string
	Status              Status
	ConsecutiveFailures int
	LastSeen            time.Time
}

// Detector monitors cluster peer liveness via periodic HTTP /internal/ping heartbeats.
type Detector struct {
	mu            sync.RWMutex
	peers         map[string]*PeerState
	client        *transport.Client
	deadThreshold int
}

func NewDetector(client *transport.Client, deadThreshold int) *Detector {
	if deadThreshold <= 0 {
		deadThreshold = 3
	}
	return &Detector{
		peers:         make(map[string]*PeerState),
		client:        client,
		deadThreshold: deadThreshold,
	}
}

func (d *Detector) RegisterPeer(nodeID, baseURL string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.peers[nodeID] = &PeerState{
		NodeID:   nodeID,
		BaseURL:  baseURL,
		Status:   StatusAlive,
		LastSeen: time.Now(),
	}
}

// ProbeOnce pings all registered peers concurrently and updates their Status.
func (d *Detector) ProbeOnce(ctx context.Context) {
	d.mu.RLock()
	targets := make([]PeerState, 0, len(d.peers))
	for _, p := range d.peers {
		targets = append(targets, *p)
	}
	d.mu.RUnlock()

	var wg sync.WaitGroup
	for _, target := range targets {
		wg.Add(1)
		go func(t PeerState) {
			defer wg.Done()
			err := d.client.Ping(ctx, t.BaseURL)

			d.mu.Lock()
			defer d.mu.Unlock()
			state, exists := d.peers[t.NodeID]
			if !exists {
				return
			}
			if err == nil {
				state.Status = StatusAlive
				state.ConsecutiveFailures = 0
				state.LastSeen = time.Now()
			} else {
				state.ConsecutiveFailures++
				if state.ConsecutiveFailures >= d.deadThreshold {
					state.Status = StatusDead
				} else {
					state.Status = StatusSuspect
				}
			}
		}(target)
	}
	wg.Wait()
}

// Start runs the periodic heartbeat loop until ctx is canceled.
func (d *Detector) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.ProbeOnce(ctx)
		}
	}
}

func (d *Detector) GetStatus(nodeID string) Status {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if p, ok := d.peers[nodeID]; ok {
		return p.Status
	}
	return StatusDead
}

func (d *Detector) IsAlive(nodeID string) bool {
	return d.GetStatus(nodeID) != StatusDead
}
