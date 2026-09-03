package raft

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"time"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
	Dead
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	case Dead:
		return "dead"
	default:
		return "unknown"
	}
}

type LogEntry struct {
	Term    int
	Index   int
	Command []byte
}

type NodeID int

type Node struct {
	mu sync.Mutex

	id       NodeID
	registry *NetworkRegistry

	currentTerm int
	votedFor    *NodeID

	logs        []LogEntry
	commitIndex int
	lastApplied int

	nextIndex  map[NodeID]int
	matchIndex map[NodeID]int
	applyCond  *sync.Cond

	role     Role
	leaderID *NodeID

	electionTimer  *time.Timer
	heartbeatEvery time.Duration

	rpcChan chan RPCEnvelope
}

func NewNode(id NodeID, registry *NetworkRegistry) *Node {
	hE := time.Duration(150+rand.IntN(151)) * time.Millisecond
	node := &Node{
		id:             id,
		registry:       registry,
		role:           Follower,
		commitIndex:    -1,
		lastApplied:    -1,
		nextIndex:      make(map[NodeID]int),
		matchIndex:     make(map[NodeID]int),
		heartbeatEvery: hE,
		electionTimer:  time.NewTimer(hE),
		rpcChan:        make(chan RPCEnvelope),
	}
	node.applyCond = sync.NewCond(&node.mu)
	return node
}

func (node *Node) Start() {
	go node.runElectionTimer()
	go node.updateLogs()

	for rpc := range node.rpcChan {
		switch msg := rpc.Payload.(type) {
		case AppendEntriesArgs:
			node.HandleAppendEntry(rpc, msg)
		case RequestVoteArgs:
			node.HandleRequestVote(rpc, msg)
		}
	}
}

func (node *Node) HandleAppendEntry(rpc RPCEnvelope, msg AppendEntriesArgs) {
	if !node.electionTimer.Stop() { // Reset timer (received a heartbeat)
		select {
		case <-node.electionTimer.C:
		default:
		}
	}
	node.mu.Lock()

	if rpc.Term >= node.currentTerm { // Peer has a newer leader
		node.leaderID = &msg.LeaderID
		node.role = Follower
		node.currentTerm = rpc.Term
	}

	reply := AppendEntriesReply{Term: node.currentTerm, Success: false}

	if msg.PrevLogIndex < 0 || (msg.PrevLogIndex < len(node.logs) && node.logs[msg.PrevLogIndex].Term == msg.PrevLogTerm) {
		node.logs = node.logs[:msg.PrevLogIndex+1]
		node.logs = append(node.logs, msg.Entries...)

		if msg.LeaderCommit > node.commitIndex {
			node.commitIndex = min(msg.LeaderCommit, msg.PrevLogIndex+1)
			node.applyCond.Signal()
		}
		reply.Success = true
	}
	node.mu.Unlock()

	rpc.ReplyChan <- reply
	node.electionTimer.Reset(node.heartbeatEvery) //  Reset heartbeat timer
}

func (node *Node) HandleRequestVote(rpc RPCEnvelope, msg RequestVoteArgs) {
	node.mu.Lock()
	defer node.mu.Unlock()

	if rpc.Term > node.currentTerm { // Newer term
		node.currentTerm = rpc.Term
		node.role = Follower
		node.votedFor = nil
	}

	reply := RequestVoteReply{Term: node.currentTerm, VoteGranted: false}

	if rpc.Term >= node.currentTerm && (node.votedFor == nil || *node.votedFor == msg.CandidateID) {
		reply.VoteGranted = true
		node.votedFor = &msg.CandidateID
		node.electionTimer.Reset(node.heartbeatEvery)
		fmt.Printf("[%d] Voted YES for %d\n", node.id, msg.CandidateID)
	} else {
		fmt.Printf("[%d] Voted NO for %d\n", node.id, msg.CandidateID)
	}
	rpc.ReplyChan <- reply
}

func (node *Node) runElectionTimer() {
	for range node.electionTimer.C {
		if node.role == Leader {
			break
		}
		node.becomeCandidate()
		go node.requestVotes()
		node.electionTimer.Reset(node.heartbeatEvery)
	}
}

func (node *Node) updateLogs() {
	node.mu.Lock()
	defer node.mu.Unlock()
	for {
		for node.lastApplied >= node.commitIndex {
			node.applyCond.Wait()
		}
		node.lastApplied++
		_ = node.logs[node.lastApplied]
	}
}

func (node *Node) startHeartbeatTicker() {
	node.broadcastHeartbeats()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		node.mu.Lock()

		if node.role != Leader {
			node.mu.Unlock()
			return
		}
		node.mu.Unlock()

		go node.broadcastHeartbeats()
	}
}

func (node *Node) broadcastHeartbeats() {
	node.mu.Lock()
	if node.role != Leader {
		node.mu.Unlock()
		return
	}

	term := node.currentTerm
	peers := node.registry.GetPeers(node.id) // Get other nodes in cluster

	type outbound struct {
		peer *Node
		args AppendEntriesArgs
	}
	sends := make([]outbound, 0, len(peers))
	for _, peer := range peers {
		prevIndex := node.nextIndex[peer.id] - 1

		prevTerm := 0
		if prevIndex >= 0 {
			prevTerm = node.logs[prevIndex].Term
		}

		entries := append([]LogEntry(nil), node.logs[node.nextIndex[peer.id]:]...)
		sends = append(sends, outbound{
			peer: peer,
			args: AppendEntriesArgs{
				LeaderID:     node.id,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  prevTerm,
				Entries:      entries,
				LeaderCommit: node.commitIndex,
			},
		})
	}
	node.mu.Unlock()

	for _, s := range sends {
		go func(peer *Node, args AppendEntriesArgs) {
			replyChan := make(chan any)
			peer.rpcChan <- RPCEnvelope{
				Term:      term,
				Payload:   args,
				ReplyChan: replyChan,
			}

			msg := <-replyChan // exactly one reply is ever sent per RPC
			reply, ok := msg.(AppendEntriesReply)
			if !ok {
				return
			}

			node.mu.Lock()
			defer node.mu.Unlock()

			if reply.Term > node.currentTerm {
				node.currentTerm = reply.Term
				node.role = Follower
				node.electionTimer.Reset(node.heartbeatEvery)
				return
			}

			if reply.Success {
				node.matchIndex[peer.id] = args.PrevLogIndex + len(args.Entries)
				node.nextIndex[peer.id] = node.matchIndex[peer.id] + 1
				node.advanceCommitIndex()
			} else if node.nextIndex[peer.id] > 1 {
				node.nextIndex[peer.id]--
			}
		}(s.peer, s.args)
	}
}

func (node *Node) advanceCommitIndex() {
	matches := make([]int, 0, len(node.matchIndex)+1)
	matches = append(matches, len(node.logs)-1)
	for _, idx := range node.matchIndex {
		matches = append(matches, idx)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(matches)))

	majorityIdx := matches[len(matches)/2] // Gets majority
	if majorityIdx > node.commitIndex && majorityIdx >= 0 && node.logs[majorityIdx].Term == node.currentTerm {
		node.commitIndex = majorityIdx
		node.applyCond.Signal()
	}
}

func (node *Node) runVoteHandler(voteChan chan any, term int, clusterSize int) {
	votes := 1 // Votes for itself
	for msg := range voteChan {
		reply, ok := msg.(RequestVoteReply)
		if !ok {
			continue
		}

		node.mu.Lock()
		stale := node.currentTerm != term
		stepDown := reply.Term > node.currentTerm
		node.mu.Unlock()

		if stepDown {
			node.becomeFollower(node.id, reply.Term)
			return
		}
		if stale {
			return // A newer election has already started
		}

		if reply.VoteGranted {
			votes++
			if votes > clusterSize/2 {
				node.becomeLeader() // Majority votes becomes leader
				go node.startHeartbeatTicker()
				return
			}
		}
	}
}

func (node *Node) becomeFollower(newLeaderID NodeID, newTerm int) {
	node.mu.Lock()

	node.leaderID = &newLeaderID
	node.role = Follower
	node.currentTerm = newTerm

	node.mu.Unlock()
}
func (node *Node) becomeCandidate() {
	node.mu.Lock()

	node.role = Candidate
	node.currentTerm += 1
	node.votedFor = &node.id

	node.mu.Unlock()
}
func (node *Node) becomeLeader() {
	if !node.electionTimer.Stop() { // Reset timer (received a heartbeat)
		select {
		case <-node.electionTimer.C:
		default:
		}
	}
	node.mu.Lock()

	node.role = Leader
	node.leaderID = &node.id

	peers := node.registry.GetPeers(node.id)
	for _, peer := range peers {
		node.nextIndex[peer.id] = len(node.logs)
		node.matchIndex[peer.id] = 0
	}

	node.mu.Unlock()
}

func (node *Node) requestVotes() {
	peers := node.registry.GetPeers(node.id) // Get other nodes in cluster
	replyChan := make(chan any, len(peers))

	for _, peer := range peers {
		go func() {
			peer.rpcChan <- RPCEnvelope{
				Term:      node.currentTerm,
				Payload:   RequestVoteArgs{CandidateID: node.id},
				ReplyChan: replyChan,
			}
		}()
	}

	go node.runVoteHandler(replyChan, node.currentTerm, len(peers)+1)
}

func (node *Node) Snapshot() (id NodeID, role Role, term int, commitIndex int, logLen int) {
	node.mu.Lock()
	defer node.mu.Unlock()
	return node.id, node.role, node.currentTerm, node.commitIndex, len(node.logs)
}

func (node *Node) Propose(cmd []byte) {
	node.mu.Lock()
	if node.role == Leader {
		entry := LogEntry{
			Term:    node.currentTerm,
			Index:   len(node.logs),
			Command: cmd,
		}
		node.logs = append(node.logs, entry)
		node.applyCond.Signal()
	}
	node.mu.Unlock()
}
