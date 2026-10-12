package raft

import (
	"fmt"
	"testing"
	"time"
)

func sendVote(node *Node, term int, candidate NodeID) RequestVoteReply {
	replyChan := make(chan any, 1)
	node.HandleRequestVote(RPCEnvelope{Term: term, ReplyChan: replyChan}, RequestVoteArgs{CandidateID: candidate})
	return (<-replyChan).(RequestVoteReply)
}

func sendAppend(node *Node, term int, args AppendEntriesArgs) AppendEntriesReply {
	replyChan := make(chan any, 1)
	node.HandleAppendEntry(RPCEnvelope{Term: term, ReplyChan: replyChan}, args)
	return (<-replyChan).(AppendEntriesReply)
}

func TestHandleRequestVote_SingleVotePerTerm(t *testing.T) {
	node := NewNode(1, NewNetworkRegistry())

	if reply := sendVote(node, 1, 2); !reply.VoteGranted {
		t.Fatalf("expected first vote in term 1 to be granted")
	}

	if reply := sendVote(node, 1, 3); reply.VoteGranted {
		t.Fatalf("expected second candidate in same term to be denied, already voted for 2")
	}

	if reply := sendVote(node, 1, 2); !reply.VoteGranted {
		t.Fatalf("expected re-vote for the same candidate in the same term to still be granted")
	}

	if reply := sendVote(node, 2, 3); !reply.VoteGranted {
		t.Fatalf("expected a higher term to reset the vote and grant to the new candidate")
	}
	if node.currentTerm != 2 {
		t.Fatalf("expected currentTerm to adopt the higher term, got %d", node.currentTerm)
	}
}

func TestHandleAppendEntry_AppendAndTruncateOnConflict(t *testing.T) {
	node := NewNode(1, NewNetworkRegistry())
	node.logs = []LogEntry{{Term: 1}, {Term: 1}}

	reply := sendAppend(node, 1, AppendEntriesArgs{
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      []LogEntry{{Term: 2}},
		LeaderCommit: 1,
	})
	if !reply.Success {
		t.Fatalf("expected append to succeed when PrevLogIndex/PrevLogTerm match")
	}
	// PrevLogIndex=1 matched, so entries 0 and 1 are kept, and the new entry lands at index 2.
	if len(node.logs) != 3 || node.logs[2].Term != 2 {
		t.Fatalf("expected new entry appended at index 2, got %+v", node.logs)
	}
	if node.commitIndex != 1 {
		t.Fatalf("expected commitIndex to advance to min(leaderCommit, ...) = 1, got %d", node.commitIndex)
	}

	reply = sendAppend(node, 1, AppendEntriesArgs{
		PrevLogIndex: 2,
		PrevLogTerm:  99, // does not match node.logs[2].Term
		Entries:      []LogEntry{{Term: 3}},
	})
	if reply.Success {
		t.Fatalf("expected append to be rejected on PrevLogTerm mismatch")
	}
	if len(node.logs) != 3 || node.logs[2].Term != 2 {
		t.Fatalf("expected logs unchanged after a rejected append, got %+v", node.logs)
	}
}

func TestAdvanceCommitIndex_MajorityAndTermCheck(t *testing.T) {
	node := NewNode(1, NewNetworkRegistry())
	node.logs = []LogEntry{{Term: 1}, {Term: 1}, {Term: 2}}
	node.currentTerm = 2
	node.matchIndex = map[NodeID]int{2: 2, 3: 1} // leader(idx 2) + peer2(idx 2) form a majority

	node.advanceCommitIndex()
	if node.commitIndex != 2 {
		t.Fatalf("expected commitIndex to advance to 2, got %d", node.commitIndex)
	}

	// A majority on an index from an older term must not be committed by count alone.
	node.commitIndex = 0
	node.currentTerm = 3
	node.advanceCommitIndex()
	if node.commitIndex != 0 {
		t.Fatalf("expected commitIndex to stay 0 when the majority index is from an older term, got %d", node.commitIndex)
	}
}

func TestBecomeLeader_InitializesPeerState(t *testing.T) {
	node := NewNode(1, NewNetworkRegistry())
	node.becomeLeader()

	if node.role != Leader {
		t.Fatalf("expected role Leader, got %v", node.role)
	}
	if node.nextIndex == nil || node.matchIndex == nil {
		t.Fatalf("expected nextIndex/matchIndex maps to be initialized, not nil")
	}
}

func startCluster(size int) []*Node {
	registry := NewNetworkRegistry()
	nodes := make([]*Node, 0, size)
	for id := NodeID(1); int(id) <= size; id++ {
		node := NewNode(id, registry)
		registry.Register(node)
		nodes = append(nodes, node)
	}
	for _, node := range nodes {
		go node.Start()
	}
	return nodes
}

func waitFor(t *testing.T, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", desc)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitForLeader waits until exactly one node in nodes is leader and returns it.
func waitForLeader(t *testing.T, nodes []*Node) *Node {
	t.Helper()
	var leader *Node
	waitFor(t, "a single leader", func() bool {
		leader = nil
		for _, node := range nodes {
			if _, role, _, _, _ := node.Snapshot(); role == Leader {
				if leader != nil {
					return false
				}
				leader = node
			}
		}
		return leader != nil
	})
	return leader
}

func termOf(node *Node) int {
	_, _, term, _, _ := node.Snapshot()
	return term
}

func logCopy(node *Node) []LogEntry {
	node.mu.Lock()
	defer node.mu.Unlock()
	return append([]LogEntry(nil), node.logs...)
}

func proposeAll(nodes []*Node, cmd string) {
	for _, node := range nodes {
		node.Propose([]byte(cmd)) // no-op on every node except the leader
	}
}

func without(nodes []*Node, exclude *Node) []*Node {
	rest := make([]*Node, 0, len(nodes)-1)
	for _, node := range nodes {
		if node != exclude {
			rest = append(rest, node)
		}
	}
	return rest
}

func TestKillRevive_ReelectsAndRevivedNodeCatchesUp(t *testing.T) {
	nodes := startCluster(3)

	oldLeader := waitForLeader(t, nodes)
	oldTerm := termOf(oldLeader)
	proposeAll(nodes, "before-kill")
	waitFor(t, "first entry to commit", func() bool {
		_, _, _, commitIndex, _ := oldLeader.Snapshot()
		return commitIndex >= 0
	})

	oldLeader.Kill()
	survivors := without(nodes, oldLeader)
	newLeader := waitForLeader(t, survivors)
	if _, role, _, _, _ := oldLeader.Snapshot(); role != Dead {
		t.Fatalf("expected killed node to stay dead while the cluster re-elects, got %v", role)
	}
	if term := termOf(newLeader); term <= oldTerm {
		t.Fatalf("expected new leader's term to exceed %d, got %d", oldTerm, term)
	}
	if log := logCopy(newLeader); len(log) == 0 || string(log[0].Command) != "before-kill" {
		t.Fatalf("expected committed entry to survive the leader's death, new leader has %+v", log)
	}

	for i := range 3 {
		proposeAll(survivors, fmt.Sprintf("while-dead-%d", i))
	}

	oldLeader.Revive()
	waitFor(t, "revived node to rejoin as a follower and match the leader's log", func() bool {
		_, role, term, _, _ := oldLeader.Snapshot()
		if role != Follower || term != termOf(newLeader) {
			return false
		}
		want, got := logCopy(newLeader), logCopy(oldLeader)
		if len(got) != len(want) {
			return false
		}
		for i := range want {
			if got[i].Term != want[i].Term || string(got[i].Command) != string(want[i].Command) {
				return false
			}
		}
		return true
	})
}

// Killing and reviving the leader repeatedly means every node eventually
// becomes leader, dies, and must later be able to stand for election again.
func TestKillRevive_RepeatedLeaderFailures(t *testing.T) {
	nodes := startCluster(3)

	leader := waitForLeader(t, nodes)
	for round := range 2 * len(nodes) {
		term := termOf(leader)
		leader.Kill()
		next := waitForLeader(t, without(nodes, leader))
		if termOf(next) <= term {
			t.Fatalf("round %d: expected term to advance past %d, got %d", round, term, termOf(next))
		}
		leader.Revive()
		leader = next
	}
}

func TestHandleAppendEntry_StaleAppendDoesNotTruncate(t *testing.T) {
	node := NewNode(1, NewNetworkRegistry())
	node.logs = []LogEntry{{Term: 1}, {Term: 1}, {Term: 1}}

	reply := sendAppend(node, 1, AppendEntriesArgs{PrevLogIndex: 0, PrevLogTerm: 1, Entries: []LogEntry{{Term: 1}}})

	if !reply.Success {
		t.Fatalf("expected append to succeed, the followers log")
	}
	if len(node.logs) != 3 {
		t.Fatalf("expected a stale append to entries, got %d", len(node.logs))
	}
}

func TestHandleAppendEntry_LowerTerm(t *testing.T) {
	node := NewNode(1, NewNetworkRegistry())
	node.logs = []LogEntry{{Term: 1}, {Term: 1}}
	node.currentTerm = 3

	reply := sendAppend(node, 2, AppendEntriesArgs{PrevLogIndex: 1, PrevLogTerm: 1, Entries: []LogEntry{{Term: 2}}})

	if reply.Success {
		t.Fatalf("expected an append from a stale term to be rejected")
	}
	if reply.Term != 3 {
		t.Fatalf("expected reply term 3 so the stale leader steps down, got %d", reply.Term)
	}
	if len(node.logs) != 2 {
		t.Fatalf("expected log unchanged after a rejected append, got %d entries", len(node.logs))
	}
}
