package raft

import (
	"Distributed_Key_Value_Store/pkg/store"
	"Distributed_Key_Value_Store/pkg/transport"
	"Distributed_Key_Value_Store/pkg/wal"
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// --- helpers ---

func newTestNode(t *testing.T) *Node {
	t.Helper()

	w, err := wal.NewWal(filepath.Join(t.TempDir(), "wal.log"))
	if err != nil {
		t.Fatal(err)
	}

	stateFile, err := os.CreateTemp(t.TempDir(), "state*.json")
	if err != nil {
		t.Fatal(err)
	}

	ps := &PersistentState{file: stateFile}

	n := &Node{
		id:              "node-1",
		role:            "follower",
		currentTerm:     0,
		votedFor:        "",
		log:             w,
		persistentState: ps,
		store:           store.NewHashMap(),
		electionTimer:   time.NewTimer(24 * time.Hour),
	}

	t.Cleanup(func() {
		w.Close()
		stateFile.Close()
	})

	return n
}

func newTestNodeWithEntries(t *testing.T, count int) *Node {
	t.Helper()
	n := newTestNode(t)
	for i := 1; i <= count; i++ {
		if err := n.log.Append(wal.NewLogEntry("SET", []string{strconv.Itoa(i), "v"}, i)); err != nil {
			t.Fatal(err)
		}
	}
	n.currentTerm = int32(count)
	return n
}

func voteReq(term int32, candidateId string, lastLogIndex, lastLogTerm int32) *transport.RequestVoteRequest {
	return &transport.RequestVoteRequest{
		Term:         term,
		CandidateId:  candidateId,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
}

// --- PersistentState ---

func TestPersistentStateWriteAndRead(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "state*.json")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	ps := &PersistentState{file: f}

	if err := ps.writeCurrentState(5, "node-2"); err != nil {
		t.Fatal(err)
	}

	if ps.CurrentTerm != 5 {
		t.Errorf("expected term 5, got %d", ps.CurrentTerm)
	}
	if ps.VotedFor != "node-2" {
		t.Errorf("expected votedFor node-2, got %q", ps.VotedFor)
	}
}

func TestPersistentStateUpdateState(t *testing.T) {
	ps := &PersistentState{}
	ps.updateState(7, "node-3")
	if ps.CurrentTerm != 7 {
		t.Errorf("expected term 7, got %d", ps.CurrentTerm)
	}
	if ps.VotedFor != "node-3" {
		t.Errorf("expected node-3, got %q", ps.VotedFor)
	}
}

func TestPersistentStateOverwrite(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "state*.json")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	ps := &PersistentState{file: f}
	ps.writeCurrentState(1, "node-a")
	ps.writeCurrentState(3, "node-b")

	if ps.CurrentTerm != 3 {
		t.Errorf("expected term 3 after overwrite, got %d", ps.CurrentTerm)
	}
	if ps.VotedFor != "node-b" {
		t.Errorf("expected node-b after overwrite, got %q", ps.VotedFor)
	}
}

// --- hasVoted / RandomElectionTime ---

func TestHasVoted(t *testing.T) {
	n := newTestNode(t)

	if n.hasVoted() {
		t.Error("expected hasVoted=false before any vote")
	}

	n.votedFor = "candidate-1"
	if !n.hasVoted() {
		t.Error("expected hasVoted=true after setting votedFor")
	}
}

func TestRandomElectionTime(t *testing.T) {
	n := newTestNode(t)
	for range 500 {
		got := n.RandomTime(150, 150)
		if got < 150 || got > 300 {
			t.Errorf("expected time in [150, 300], got %d", got)
		}
	}
}

// --- RequestVote ---

func TestRequestVote(t *testing.T) {
	t.Run("lower term is rejected", func(t *testing.T) {
		n := newTestNode(t)
		n.currentTerm = 5
		res, err := n.RequestVote(context.Background(), voteReq(3, "c1", 0, 0))
		if err != nil {
			t.Fatal(err)
		}
		if res.VoteGranted {
			t.Error("expected vote denied for lower term")
		}
		if res.Term != 5 {
			t.Errorf("expected response term 5, got %d", res.Term)
		}
	})

	t.Run("higher term causes step down to follower", func(t *testing.T) {
		n := newTestNode(t)
		n.currentTerm = 1
		n.role = "leader"
		_, err := n.RequestVote(context.Background(), voteReq(5, "c1", 0, 0))
		if err != nil {
			t.Fatal(err)
		}
		if n.role != "follower" {
			t.Errorf("expected follower after seeing higher term, got %q", n.role)
		}
		if n.currentTerm != 5 {
			t.Errorf("expected term updated to 5, got %d", n.currentTerm)
		}
	})

	t.Run("grants vote when log is up to date", func(t *testing.T) {
		n := newTestNode(t)
		n.currentTerm = 1
		res, err := n.RequestVote(context.Background(), voteReq(1, "c1", 0, 0))
		if err != nil {
			t.Fatal(err)
		}
		if !res.VoteGranted {
			t.Error("expected vote granted for up-to-date log")
		}
		if n.votedFor != "c1" {
			t.Errorf("expected votedFor c1, got %q", n.votedFor)
		}
		if n.role != "follower" {
			t.Errorf("expected role follower after granting vote, got %q", n.role)
		}
	})

	t.Run("denies vote when already voted for a different candidate", func(t *testing.T) {
		n := newTestNode(t)
		n.currentTerm = 1
		n.votedFor = "other"
		res, err := n.RequestVote(context.Background(), voteReq(1, "c1", 0, 0))
		if err != nil {
			t.Fatal(err)
		}
		if res.VoteGranted {
			t.Error("expected vote denied when already voted for different candidate")
		}
	})

	t.Run("re-grants vote to the same candidate", func(t *testing.T) {
		n := newTestNode(t)
		n.currentTerm = 1
		n.votedFor = "c1"
		res, err := n.RequestVote(context.Background(), voteReq(1, "c1", 0, 0))
		if err != nil {
			t.Fatal(err)
		}
		if !res.VoteGranted {
			t.Error("expected vote re-granted for same candidate")
		}
	})

	t.Run("denies vote when candidate log is behind", func(t *testing.T) {
		n := newTestNodeWithEntries(t, 5)
		// node has 5 entries at term 5; candidate only has 2 entries at term 2
		res, err := n.RequestVote(context.Background(), voteReq(5, "c1", 2, 2))
		if err != nil {
			t.Fatal(err)
		}
		if res.VoteGranted {
			t.Error("expected vote denied when candidate log is behind")
		}
	})

	t.Run("grants vote when candidate has higher last log term", func(t *testing.T) {
		n := newTestNodeWithEntries(t, 3)
		// node has last term 3; candidate claims last term 5
		res, err := n.RequestVote(context.Background(), voteReq(5, "c1", 3, 5))
		if err != nil {
			t.Fatal(err)
		}
		if !res.VoteGranted {
			t.Error("expected vote granted when candidate has higher last log term")
		}
	})

	t.Run("denies vote when candidate has lower last log term", func(t *testing.T) {
		n := newTestNodeWithEntries(t, 5)
		n.currentTerm = 5
		// node last term is 5; candidate claims last term 2
		res, err := n.RequestVote(context.Background(), voteReq(5, "c1", 10, 2))
		if err != nil {
			t.Fatal(err)
		}
		if res.VoteGranted {
			t.Error("expected vote denied when candidate has lower last log term")
		}
	})
}

// --- handleHeartBeat ---

func TestHandleHeartBeat(t *testing.T) {
	t.Run("same term returns success", func(t *testing.T) {
		n := newTestNode(t)
		n.currentTerm = 2
		res, err := n.handleHeartBeat(&transport.AppendEntriesRequest{Term: 2, LeaderId: "leader-1"})
		if err != nil {
			t.Fatal(err)
		}
		if !res.Success {
			t.Error("expected success for same-term heartbeat")
		}
		if res.Term != 2 {
			t.Errorf("expected term 2, got %d", res.Term)
		}
	})

	t.Run("lower term returns failure", func(t *testing.T) {
		n := newTestNode(t)
		n.currentTerm = 5
		res, err := n.handleHeartBeat(&transport.AppendEntriesRequest{Term: 2, LeaderId: "leader-1"})
		if err != nil {
			t.Fatal(err)
		}
		if res.Success {
			t.Error("expected failure for lower-term heartbeat")
		}
		if res.Term != 5 {
			t.Errorf("expected term 5 in response, got %d", res.Term)
		}
	})

	t.Run("higher term steps down and returns success", func(t *testing.T) {
		n := newTestNode(t)
		n.currentTerm = 1
		n.role = "candidate"
		res, err := n.handleHeartBeat(&transport.AppendEntriesRequest{Term: 4, LeaderId: "leader-1"})
		if err != nil {
			t.Fatal(err)
		}
		if !res.Success {
			t.Error("expected success after stepping down to higher term")
		}
		if n.role != "follower" {
			t.Errorf("expected follower after step down, got %q", n.role)
		}
		if n.currentTerm != 4 {
			t.Errorf("expected term updated to 4, got %d", n.currentTerm)
		}
	})
}

// --- AppendEntries routing ---

func TestAppendEntriesRoutesToHeartbeat(t *testing.T) {
	n := newTestNode(t)
	n.currentTerm = 1
	req := &transport.AppendEntriesRequest{Term: 1, Entries: []string{}}
	res, err := n.AppendEntries(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Success {
		t.Error("expected heartbeat success for empty entries")
	}
}

// --- handleEntry ---

func TestHandleEntry(t *testing.T) {
	t.Run("lower term returns failure", func(t *testing.T) {
		n := newTestNodeWithEntries(t, 5)
		req := &transport.AppendEntriesRequest{
			Term:         2,
			Entries:      []string{"SET:k,v:2"},
			PrevLogIndex: 5,
			PrevLogTerm:  5,
		}
		res, err := n.handleEntry(req)
		if err != nil {
			t.Fatal(err)
		}
		if res.Success {
			t.Error("expected failure for lower term")
		}
	})

	t.Run("prev log term mismatch returns failure", func(t *testing.T) {
		n := newTestNodeWithEntries(t, 5)
		req := &transport.AppendEntriesRequest{
			Term:         5,
			Entries:      []string{"SET:k,v:5"},
			PrevLogIndex: 3,
			PrevLogTerm:  99,
		}
		res, err := n.handleEntry(req)
		if err != nil {
			t.Fatal(err)
		}
		if res.Success {
			t.Error("expected failure for prev log term mismatch")
		}
	})

	t.Run("higher term causes step down", func(t *testing.T) {
		n := newTestNodeWithEntries(t, 3)
		n.role = "candidate"
		req := &transport.AppendEntriesRequest{
			Term:         10,
			Entries:      []string{"SET:k,v:10"},
			PrevLogIndex: 3,
			PrevLogTerm:  3,
			LeaderCommit: 4,
		}
		n.handleEntry(req)
		if n.role != "follower" {
			t.Errorf("expected follower after higher term, got %q", n.role)
		}
	})

	t.Run("successful append adds entry and advances commit", func(t *testing.T) {
		n := newTestNodeWithEntries(t, 3)
		req := &transport.AppendEntriesRequest{
			Term:         3,
			Entries:      []string{"SET:newkey,newval:3"},
			PrevLogIndex: 3,
			PrevLogTerm:  3,
			LeaderCommit: 4,
		}
		res, err := n.handleEntry(req)
		if err != nil {
			t.Fatal(err)
		}
		if !res.Success {
			t.Errorf("expected success for valid append, got failure (term=%d)", res.Term)
		}
		if n.log.LastLogIndex() != 4 {
			t.Errorf("expected 4 entries after append, got %d", n.log.LastLogIndex())
		}
		if n.commitIndex != 4 {
			t.Errorf("expected commitIndex 4, got %d", n.commitIndex)
		}
	})

	t.Run("committed SET entry is applied to store", func(t *testing.T) {
		n := newTestNodeWithEntries(t, 2)
		req := &transport.AppendEntriesRequest{
			Term:         2,
			Entries:      []string{"SET:color,blue:2"},
			PrevLogIndex: 2,
			PrevLogTerm:  2,
			LeaderCommit: 3,
		}
		_, err := n.handleEntry(req)
		if err != nil {
			t.Fatal(err)
		}
		val, ok := n.store.Get("color")
		if !ok {
			t.Error("expected SET entry to be applied to store")
		}
		if val != "blue" {
			t.Errorf("expected store value blue, got %q", val)
		}
	})
}

// --- tallyVotes ---

func TestTallyVotes(t *testing.T) {
	t.Run("counts granted votes", func(t *testing.T) {
		n := newTestNode(t)
		n.currentTerm = 1
		n.peers = []string{"p1", "p2", "p3", "p4"}

		ch := make(chan *transport.RequestVoteResponse, 4)
		ch <- &transport.RequestVoteResponse{VoteGranted: true, Term: 1}
		ch <- &transport.RequestVoteResponse{VoteGranted: true, Term: 1}
		ch <- &transport.RequestVoteResponse{VoteGranted: false, Term: 1}
		ch <- &transport.RequestVoteResponse{VoteGranted: false, Term: 1}

		votes, err := n.tallyVotes(ch)
		if err != nil {
			t.Fatal(err)
		}
		if votes != 3 {
			t.Errorf("expected 3 votes including self vote, got %d", votes)
		}
	})

	t.Run("nil response is skipped", func(t *testing.T) {
		n := newTestNode(t)
		n.currentTerm = 1
		n.peers = []string{"p1", "p2"}

		ch := make(chan *transport.RequestVoteResponse, 2)
		ch <- nil
		ch <- &transport.RequestVoteResponse{VoteGranted: true, Term: 1}

		votes, err := n.tallyVotes(ch)
		if err != nil {
			t.Fatal(err)
		}
		if votes != 2 {
			t.Errorf("expected 2 votes including self vote (nil skipped), got %d", votes)
		}
	})

	t.Run("higher term response causes step down", func(t *testing.T) {
		n := newTestNode(t)
		n.currentTerm = 1
		n.role = "candidate"
		n.peers = []string{"p1"}

		ch := make(chan *transport.RequestVoteResponse, 1)
		ch <- &transport.RequestVoteResponse{VoteGranted: false, Term: 10}

		_, err := n.tallyVotes(ch)
		if err != nil {
			t.Fatal(err)
		}
		if n.currentTerm != 10 {
			t.Errorf("expected term updated to 10, got %d", n.currentTerm)
		}
		if n.role != "follower" {
			t.Errorf("expected follower after step down, got %q", n.role)
		}
	})
}

// --- stepDown ---

func TestStepDown(t *testing.T) {
	n := newTestNode(t)
	n.currentTerm = 3
	n.role = "leader"
	n.votedFor = "self"

	if err := n.stepDown(7); err != nil {
		t.Fatal(err)
	}

	if n.currentTerm != 7 {
		t.Errorf("expected term 7, got %d", n.currentTerm)
	}
	if n.role != "follower" {
		t.Errorf("expected follower, got %q", n.role)
	}
	if n.votedFor != "" {
		t.Errorf("expected votedFor cleared, got %q", n.votedFor)
	}
}
