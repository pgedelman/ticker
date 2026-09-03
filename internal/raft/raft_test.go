package raft

import "testing"

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
