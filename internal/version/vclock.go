package version

import (
	"bytes"
	"maps"
)

// Ordering represents the causal relationship between two VectorClocks.
type Ordering int

const (
	Equal Ordering = iota
	Before
	After
	Concurrent
)

// VectorClock maps nodeID -> monotonic logical counter.
type VectorClock map[string]uint64

func New() VectorClock {
	return make(VectorClock)
}

func (vc VectorClock) Clone() VectorClock {
	if vc == nil {
		return New()
	}
	return maps.Clone(vc)
}

// Increment advances the logical counter for `nodeID` by 1.
func (vc VectorClock) Increment(nodeID string) VectorClock {
	next := vc.Clone()
	next[nodeID]++
	return next
}

// Merge computes the element-wise maximum of two vector clocks.
func (vc VectorClock) Merge(other VectorClock) VectorClock {
	merged := vc.Clone()
	for node, count := range other {
		if count > merged[node] {
			merged[node] = count
		}
	}
	return merged
}

// Compare returns the causal Ordering of `vc` relative to `other`.
func (vc VectorClock) Compare(other VectorClock) Ordering {
	var vcGreater, otherGreater bool

	for node, vCount := range vc {
		oCount := other[node]
		if vCount > oCount {
			vcGreater = true
		} else if vCount < oCount {
			otherGreater = true
		}
	}
	for node, oCount := range other {
		if _, exists := vc[node]; !exists && oCount > 0 {
			otherGreater = true
		}
	}

	switch {
	case !vcGreater && !otherGreater:
		return Equal
	case vcGreater && !otherGreater:
		return After
	case !vcGreater && otherGreater:
		return Before
	default:
		return Concurrent
	}
}

// VersionedRecord wraps a key's value (or deletion tombstone) with its causal VectorClock and wall-clock LWW tie-breaker.
type VersionedRecord struct {
	Key         string      `json:"key"`
	Value       []byte      `json:"value"`
	Tombstone   bool        `json:"tombstone"`
	Clock       VectorClock `json:"clock"`
	TimestampNs int64       `json:"timestamp_ns"`
}

// ResolveWinner compares two VersionedRecords and returns the causally latest record.
// If the two records are causally Concurrent, it merges their VectorClocks and uses
// TimestampNs (Last-Write-Wins) with deterministic byte tie-breaking.
func ResolveWinner(a, b VersionedRecord) VersionedRecord {
	switch a.Clock.Compare(b.Clock) {
	case After, Equal:
		return a
	case Before:
		return b
	case Concurrent:
		winner := a
		if b.TimestampNs > a.TimestampNs {
			winner = b
		} else if b.TimestampNs == a.TimestampNs {
			if b.Tombstone && !a.Tombstone {
				winner = b
			} else if bytes.Compare(b.Value, a.Value) > 0 {
				winner = b
			}
		}
		winner.Clock = a.Clock.Merge(b.Clock)
		return winner
	default:
		return a
	}
}
