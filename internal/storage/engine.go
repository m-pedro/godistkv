package storage

import (
	"sync"

	"github.com/m-pedro/godistkv/internal/version"
)

// Engine is a thread-safe versioned replica key-value store.
type Engine struct {
	mu   sync.RWMutex
	data map[string]version.VersionedRecord
}

func NewEngine() *Engine {
	return &Engine{
		data: make(map[string]version.VersionedRecord),
	}
}

// Put applies `incoming` using causal VectorClock + LWW resolution.
// Returns the winning VersionedRecord stored on this replica and a bool indicating if incoming won.
func (e *Engine) Put(incoming version.VersionedRecord) (version.VersionedRecord, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	cloned := cloneRecord(incoming)
	existing, ok := e.data[cloned.Key]
	if !ok {
		e.data[cloned.Key] = cloned
		return cloned, true
	}

	// If existing causally dominates incoming, reject the stale write
	if existing.Clock.Compare(cloned.Clock) == version.After {
		return cloneRecord(existing), false
	}

	winner := version.ResolveWinner(existing, cloned)
	e.data[cloned.Key] = cloneRecord(winner)
	return cloneRecord(winner), true
}

// Get retrieves the VersionedRecord for `key` (including tombstones so coordinators can reconcile deletes).
func (e *Engine) Get(key string) (version.VersionedRecord, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	rec, ok := e.data[key]
	if !ok {
		return version.VersionedRecord{}, false
	}
	return cloneRecord(rec), true
}

// Len returns the total number of versioned records (including tombstones) stored on this replica.
func (e *Engine) Len() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.data)
}

func cloneRecord(r version.VersionedRecord) version.VersionedRecord {
	valCopy := make([]byte, len(r.Value))
	copy(valCopy, r.Value)
	return version.VersionedRecord{
		Key:         r.Key,
		Value:       valCopy,
		Tombstone:   r.Tombstone,
		Clock:       r.Clock.Clone(),
		TimestampNs: r.TimestampNs,
	}
}
