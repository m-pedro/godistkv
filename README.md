# godistkv

A Dynamo-style distributed consistent-hashing key-value cluster in Go featuring virtual nodes (`vnodes`), tunable quorum replication ($N, R, W$), vector-clock causal versioning, asynchronous read repair, and hinted handoff—built with **100% Go standard library**.

## 15-Day Incremental Engineering Roadmap (Month 2, Days 46–60)

- [x] **Day 46:** Initialize `godistkv` module, Dynamo architecture spec, and 15-day roadmap
- [ ] **Day 47:** Implement FNV-1a consistent hash ring with virtual nodes and replica preference lists (`internal/ring`)
- [ ] **Day 48:** Verify virtual-node load balance and minimal key remapping on node churn (`internal/ring/ring_test.go`)
- [ ] **Day 49:** Implement vector clocks, causal comparison, and LWW conflict resolution (`internal/version`)
- [ ] **Day 50:** Implement thread-safe versioned replica store with causal upsert and tombstones (`internal/storage`)
- [ ] **Day 51:** Add peer-to-peer HTTP replica RPC server and context-aware client (`internal/transport`)
- [ ] **Day 52:** Implement parallel scatter-gather $W$-quorum write coordinator (`internal/coordinator`)
- [ ] **Day 53:** Implement $R$-quorum read coordinator with version reconciliation
- [ ] **Day 54:** Add asynchronous read-repair to heal stale replicas on quorum reads
- [ ] **Day 55:** Implement periodic heartbeat prober and node liveness state machine (`internal/membership`)
- [ ] **Day 56:** Implement sloppy quorum hinted handoff store and background replay worker
- [ ] **Day 57:** Add multi-node chaos test (node kill, quorum survival, and hinted handoff recovery)
- [ ] **Day 58:** Build multi-node cluster runner and public HTTP gateway (`cmd/godistkv/main.go`)
- [ ] **Day 59:** Add consistent hash lookup and concurrent quorum write benchmarks
- [ ] **Day 60:** Complete Month 2 Dynamo architecture guide, ASCII diagrams, and benchmark table
