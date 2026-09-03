# ticker

A Raft consensus implementation in Go: leader election, heartbeats, log replication, and majority-based commit advancement, run against an in-memory cluster.

## What's here

`internal/raft` is the implementation. A `Node` runs as a follower, candidate, or leader, talking to peers over Go channels through a `NetworkRegistry` (`registry.go` has the RPC types and registry, `raft.go` has the node logic). Each node:

- runs a randomized election timer and becomes a candidate on timeout
- requests votes from peers and becomes leader on a majority
- once leader, sends periodic heartbeats and replicates entries proposed via `Propose()`
- advances its commit index once an entry is acknowledged by a majority of the cluster

`cmd/node/main.go` is a small driver: it starts a 3-node cluster, lets an election settle, proposes an entry, and prints where each node landed.

## Running it

```
go build ./...
go test ./...
go run ./cmd/node
```

## Notes

Node-to-node communication is in-memory (goroutines and channels via `NetworkRegistry`), not a real network transport — the goal here was getting the consensus algorithm itself right against the Raft paper (Ongaro & Ousterhout, "In Search of an Understandable Consensus Algorithm") before adding a wire protocol on top of it.
