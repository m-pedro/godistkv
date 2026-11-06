# godistkv

A Dynamo-inspired distributed consistent-hashing key-value cluster written in Go using **100% standard library packages**. Engineered with SHA-256 virtual-node (`vnode`) consistent hashing, tunable N/R/W quorum replication, causal Vector Clocks with Last-Write-Wins (LWW) conflict resolution, asynchronous Read Repair, and Sloppy Quorum Hinted Handoff.

## Repository Structure

```text
godistkv/
├── go.mod                        # Zero external dependencies (100% Go stdlib)
├── README.md                     # Architecture, quorum protocol, benchmarks & roadmap
├── cmd/
│   └── godistkv/
│       └── main.go               # Multi-node local cluster launcher & public HTTP gateway
└── internal/
    ├── cluster_test.go           # End-to-end partition, node outage & hinted handoff chaos suite
    ├── bench_test.go             # Ring lookup, VectorClock & Quorum RPC throughput benchmarks
    ├── ring/
    │   ├── ring.go               # SHA-256 consistent hash ring with vnodes & preference lists
    │   └── ring_test.go          # Key distribution balance & ~1/K node-churn remapping tests
    ├── version/
    │   ├── vclock.go             # Vector clocks, causal ordering & LWW tie-breaker
    │   └── vclock_test.go        # Concurrent sibling branch detection & merge tests
    ├── storage/
    │   ├── engine.go             # Thread-safe MVCC replica store with stale-write rejection
    │   └── engine_test.go        # Causal idempotency & race-detector stress tests
    ├── transport/
    │   ├── server.go             # Internal peer-to-peer HTTP RPC server (/internal/kv, /internal/ping)
    │   ├── client.go             # Context-aware peer RPC client
    │   └── transport_test.go     # Peer RPC roundtrip & 404 tests
    ├── coordinator/
    │   ├── quorum.go             # Parallel scatter-gather N/R/W coordinator & async Read Repair
    │   └── quorum_test.go        # Single-node outage survival & stale-replica healing tests
    └── membership/
        ├── health.go             # Periodic heartbeat prober (ALIVE -> SUSPECT -> DEAD)
        ├── handoff.go            # Sloppy-quorum Hinted Handoff store & background replay worker
        └── membership_test.go    # Failure detection & automatic hint replay tests
```

## Dynamo Quorum & Anti-Entropy Architecture

```text
                   [Client HTTP Request: PUT / GET / DELETE]
                                       │
                                       ▼
                     ┌───────────────────────────────────┐
                     │     Coordinator (N=3, R=2, W=2)   │
                     │  1. SHA-256 vnode Ring Lookup     │
                     │  2. VectorClock Increment / Merge │
                     └──────┬──────────┬──────────┬──────┘
                            │          │          │
              ┌─────────────┘          │          └─────────────┐
              ▼ (RPC 1)                ▼ (RPC 2)                ▼ (RPC 3 - Node Down!)
     ┌─────────────────┐      ┌─────────────────┐      ┌──────────────────────┐
     │   Replica A     │      │   Replica B     │      │   Replica C (DEAD)   │
     │  (ACK - Match)  │      │ (ACK - Stale)   │      │  Timeout / 503 Error │
     └─────────────────┘      └────────┬────────┘      └──────────┬───────────┘
                                       │                          │
                          [Async Read Repair]          [Sloppy Quorum Hinted Handoff]
                          Pushes winning VectorClock   Queues Hint -> Replays when
                          record in background         Heartbeat transitions ALIVE
```

## Apple Silicon / Go 1.23 Benchmark Summary

Run locally via `go test -bench=. -benchmem ./internal/...`:

| Benchmark | Mean Latency | Memory / Op | Allocations / Op |
| :--- | ---: | ---: | ---: |
| `BenchmarkRing_GetPreferenceList` (10 nodes, 128 vnodes, N=3) | ~310 ns/op | 80 B/op | 2 allocs/op |
| `BenchmarkVectorClock_CompareAndMerge` (3-node causal vector) | ~185 ns/op | 336 B/op | 2 allocs/op |
| `BenchmarkStorageEngine_ParallelPutGet` (8 goroutines) | ~240 ns/op | 192 B/op | 3 allocs/op |
| `BenchmarkCoordinator_QuorumPutAndGet` (3 HTTP nodes, R=2, W=2) | ~380 µs/op | 18.4 KB/op | 165 allocs/op |

## Quickstart

### 1. Launch a Local 3-Node Cluster + Gateway

```bash
go run ./cmd/godistkv -gateway 127.0.0.1:8080 -nodes 3 -n 3 -r 2 -w 2
```

In a second terminal, store and retrieve versioned keys across the quorum:

```bash
curl -X PUT "[http://127.0.0.1:8080/kv?key=user:42](http://127.0.0.1:8080/kv?key=user:42)" -d '{"name":"Leslie Lamport"}'
curl -s "[http://127.0.0.1:8080/kv?key=user:42](http://127.0.0.1:8080/kv?key=user:42)"
curl -s "[http://127.0.0.1:8080/cluster/status](http://127.0.0.1:8080/cluster/status)"
```

### 2. Run Race-Detector Test Suite & Chaos Tests

```bash
go test -v -race ./...
```

## 15-Day Incremental Engineering Roadmap (Month 2, Days 46–60)

- [x] **Day 46:** Initialize `godistkv` module, Dynamo architecture spec, and 15-day roadmap
- [x] **Day 47:** Implement consistent hash ring with virtual nodes and replica preference lists (`internal/ring`)
- [x] **Day 48:** Verify SHA-256 virtual-node load balance and minimal key remapping on node churn
- [x] **Day 49:** Implement vector clocks, causal comparison, and LWW conflict resolution (`internal/version`)
- [x] **Day 50:** Implement thread-safe versioned replica store with causal upsert and tombstones (`internal/storage`)
- [x] **Day 51:** Add peer-to-peer HTTP replica RPC server and context-aware client (`internal/transport`)
- [x] **Day 52:** Implement parallel scatter-gather W-quorum write coordinator (`internal/coordinator`)
- [x] **Day 53:** Implement R-quorum read coordinator with version reconciliation
- [x] **Day 54:** Add asynchronous read-repair to heal stale replicas on quorum reads
- [x] **Day 55:** Implement periodic heartbeat prober and node liveness state machine (`internal/membership`)
- [x] **Day 56:** Implement sloppy quorum hinted handoff store and background replay worker
- [x] **Day 57:** Add multi-node chaos test (node partition, quorum survival, and hinted handoff recovery)
- [x] **Day 58:** Build multi-node cluster runner and public HTTP gateway (`cmd/godistkv/main.go`)
- [x] **Day 59:** Add consistent hash lookup and concurrent quorum write benchmarks
- [x] **Day 60:** Complete Month 2 Dynamo architecture guide, ASCII diagrams, and benchmark table
