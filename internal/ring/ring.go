package ring

import (
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
)

var (
	ErrEmptyRing      = errors.New("consistent hash ring has no nodes")
	ErrInvalidVnodes  = errors.New("virtual node count must be > 0")
)

type vnode struct {
	token  uint32
	nodeID string
}

// Ring implements a thread-safe consistent hashing ring with virtual nodes.
type Ring struct {
	mu          sync.RWMutex
	vnodeCount  int
	vnodes      []vnode
	nodes       map[string]struct{}
}

func New(vnodeCount int) (*Ring, error) {
	if vnodeCount <= 0 {
		return nil, ErrInvalidVnodes
	}
	return &Ring{
		vnodeCount: vnodeCount,
		nodes:      make(map[string]struct{}),
	}, nil
}

// AddNode inserts a physical node and its virtual tokens onto the ring.
func (r *Ring) AddNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.nodes[nodeID]; exists {
		return
	}
	r.nodes[nodeID] = struct{}{}

	for i := 0; i < r.vnodeCount; i++ {
		vkey := fmt.Sprintf("%s#%d", nodeID, i)
		r.vnodes = append(r.vnodes, vnode{
			token:  HashKey(vkey),
			nodeID: nodeID,
		})
	}

	sort.Slice(r.vnodes, func(i, j int) bool {
		if r.vnodes[i].token == r.vnodes[j].token {
			return r.vnodes[i].nodeID < r.vnodes[j].nodeID
		}
		return r.vnodes[i].token < r.vnodes[j].token
	})
}

// RemoveNode removes a physical node and all of its virtual tokens from the ring.
func (r *Ring) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.nodes[nodeID]; !exists {
		return
	}
	delete(r.nodes, nodeID)

	filtered := r.vnodes[:0]
	for _, vn := range r.vnodes {
		if vn.nodeID != nodeID {
			filtered = append(filtered, vn)
		}
	}
	r.vnodes = filtered
}

// GetPreferenceList returns up to `n` distinct physical node IDs responsible for `key`,
// walking clockwise around the ring starting from HashKey(key).
func (r *Ring) GetPreferenceList(key string, n int) ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.nodes) == 0 {
		return nil, ErrEmptyRing
	}
	if n > len(r.nodes) {
		n = len(r.nodes)
	}
	if n <= 0 {
		return nil, nil
	}

	token := HashKey(key)
	startIdx := sort.Search(len(r.vnodes), func(i int) bool {
		return r.vnodes[i].token >= token
	})
	if startIdx == len(r.vnodes) {
		startIdx = 0
	}

	result := make([]string, 0, n)
	seen := make(map[string]struct{}, n)

	for i := 0; i < len(r.vnodes) && len(result) < n; i++ {
		idx := (startIdx + i) % len(r.vnodes)
		candidate := r.vnodes[idx].nodeID
		if _, alreadyPicked := seen[candidate]; !alreadyPicked {
			seen[candidate] = struct{}{}
			result = append(result, candidate)
		}
	}

	return result, nil
}

// Nodes returns a sorted slice of all physical node IDs currently in the ring.
func (r *Ring) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]string, 0, len(r.nodes))
	for id := range r.nodes {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// HashKey computes the 32-bit FNV-1a hash of a key.
func HashKey(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}
