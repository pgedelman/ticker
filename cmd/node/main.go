package main

import (
	"fmt"
	"time"

	"ticker/internal/raft"
)

func main() {
	registry := raft.NewNetworkRegistry()

	nodes := make([]*raft.Node, 0, 3)
	for id := raft.NodeID(1); id <= 3; id++ {
		node := raft.NewNode(id, registry)
		registry.Register(node)
		nodes = append(nodes, node)
	}

	for _, node := range nodes {
		go node.Start()
	}

	time.Sleep(500 * time.Millisecond) // let an election settle

	for _, node := range nodes {
		node.Propose([]byte("hello")) // no-op on every node except the leader
	}

	time.Sleep(200 * time.Millisecond) // let the leader replicate it

	for _, node := range nodes {
		id, role, term, commitIndex, logLen := node.Snapshot()
		fmt.Printf("node %d: role=%s term=%d commitIndex=%d logLen=%d\n", id, role, term, commitIndex, logLen)
	}
}
