package raft

import "sync"

type AppendEntriesArgs struct {
	LeaderID     NodeID
	PrevLogTerm  int
	PrevLogIndex int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  NodeID
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type RPCEnvelope struct {
	Term      int
	Payload   any
	ReplyChan chan any
}

type NetworkRegistry struct {
	mu    sync.RWMutex
	nodes map[NodeID]*Node
}

func NewNetworkRegistry() *NetworkRegistry {
	registry := &NetworkRegistry{
		nodes: make(map[NodeID]*Node),
	}
	return registry
}

func (registry *NetworkRegistry) GetPeers(nid NodeID) []*Node {
	registry.mu.RLock()
	peers := make([]*Node, 0, len(registry.nodes))
	for id, peer := range registry.nodes {
		if nid != id {
			peers = append(peers, peer)
		}
	}
	registry.mu.RUnlock()
	return peers
}

func (registry *NetworkRegistry) Register(node *Node) {
	registry.mu.Lock()
	registry.nodes[node.id] = node
	registry.mu.Unlock()
}

func (registry *NetworkRegistry) RegisterNewNode(id NodeID) {
	registry.mu.Lock()
	registry.nodes[id] = NewNode(id, registry)
	registry.mu.Unlock()
}
