package raft

import (
	"Distributed_Key_Value_Store/pkg/transport"
	"testing"
)

func TestInitLeaderState(t *testing.T) {
	n := newTestNodeWithEntries(t, 3)
	p1 := newTestNode(t)
	p2 := newTestNode(t)
	p1.id = "p1"
	p2.id = "p2"

	n.peers = []string{p1.id, p2.id}
	n.initLeaderState()

	for _, peer := range n.peers {
		if got := n.nextIndex[peer]; got != 4 {
			t.Errorf("expected nextIndex[%s] to be 4, got %d", peer, got)
		}
		if got := n.matchIndex[peer]; got != 0 {
			t.Errorf("expected matchIndex[%s] to be 0, got %d", peer, got)
		}
	}
}

func TestNewAppendEntriesRequest(t *testing.T) {
	n := newTestNodeWithEntries(t, 4)
	n.id = "leader"
	n.currentTerm = 4
	n.commitIndex = 2
	n.nextIndex = map[string]int32{"p1": 3}

	req, err := n.newAppendEntriesRequest("p1")
	if err != nil {
		t.Fatal(err)
	}

	if req.Term != 4 {
		t.Errorf("expected term 4, got %d", req.Term)
	}
	if req.LeaderId != "leader" {
		t.Errorf("expected leader id %q, got %q", "leader", req.LeaderId)
	}
	if req.PrevLogIndex != 2 {
		t.Errorf("expected prevLogIndex 2, got %d", req.PrevLogIndex)
	}
	if req.PrevLogTerm != 2 {
		t.Errorf("expected prevLogTerm 2, got %d", req.PrevLogTerm)
	}
	if req.LeaderCommit != 2 {
		t.Errorf("expected leaderCommit 2, got %d", req.LeaderCommit)
	}
	if len(req.Entries) != 2 {
		t.Fatalf("expected 2 entries from index 3, got %d", len(req.Entries))
	}
	if req.Entries[0] != "SET:3,v:3" {
		t.Errorf("expected first entry from index 3, got %q", req.Entries[0])
	}
	if req.Entries[1] != "SET:4,v:4" {
		t.Errorf("expected second entry from index 4, got %q", req.Entries[1])
	}
}

func TestHandleEntryResult(t *testing.T) {
	t.Run("success updates matchIndex and nextIndex", func(t *testing.T) {
		n := newTestNodeWithEntries(t, 4)
		n.currentTerm = 4
		n.nextIndex = map[string]int32{"p1": 3}
		n.matchIndex = map[string]int32{"p1": 0}

		err := n.handleAppendEntriesResult(&appendEntryResult{
			peer:     "p1",
			request:  &transport.AppendEntriesRequest{PrevLogIndex: 2, Entries: []string{"SET:3,v:3", "SET:4,v:4"}},
			response: &transport.AppendEntriesResponse{Term: 4, Success: true},
		})
		if err != nil {
			t.Fatal(err)
		}

		if got := n.matchIndex["p1"]; got != 4 {
			t.Errorf("expected matchIndex[p1] to be 4, got %d", got)
		}
		if got := n.nextIndex["p1"]; got != 5 {
			t.Errorf("expected nextIndex[p1] to be 5, got %d", got)
		}
	})

	t.Run("failure decrements nextIndex", func(t *testing.T) {
		n := newTestNodeWithEntries(t, 4)
		n.currentTerm = 4
		n.nextIndex = map[string]int32{"p1": 3}
		n.matchIndex = map[string]int32{"p1": 0}

		err := n.handleAppendEntriesResult(&appendEntryResult{
			peer:     "p1",
			request:  &transport.AppendEntriesRequest{PrevLogIndex: 2},
			response: &transport.AppendEntriesResponse{Term: 4, Success: false},
		})
		if err != nil {
			t.Fatal(err)
		}

		if got := n.nextIndex["p1"]; got != 2 {
			t.Errorf("expected nextIndex[p1] to be 2, got %d", got)
		}
		if got := n.matchIndex["p1"]; got != 0 {
			t.Errorf("expected matchIndex[p1] to remain 0, got %d", got)
		}
	})

	t.Run("failure does not decrement nextIndex below one", func(t *testing.T) {
		n := newTestNodeWithEntries(t, 4)
		n.currentTerm = 4
		n.nextIndex = map[string]int32{"p1": 1}
		n.matchIndex = map[string]int32{"p1": 0}

		err := n.handleAppendEntriesResult(&appendEntryResult{
			peer:     "p1",
			request:  &transport.AppendEntriesRequest{PrevLogIndex: 0},
			response: &transport.AppendEntriesResponse{Term: 4, Success: false},
		})
		if err != nil {
			t.Fatal(err)
		}

		if got := n.nextIndex["p1"]; got != 1 {
			t.Errorf("expected nextIndex[p1] to remain 1, got %d", got)
		}
	})

	t.Run("higher term causes step down", func(t *testing.T) {
		n := newTestNodeWithEntries(t, 4)
		n.currentTerm = 4
		n.role = "leader"
		n.votedFor = "node-1"
		n.nextIndex = map[string]int32{"p1": 3}
		n.matchIndex = map[string]int32{"p1": 0}

		err := n.handleAppendEntriesResult(&appendEntryResult{
			peer:     "p1",
			request:  &transport.AppendEntriesRequest{PrevLogIndex: 2},
			response: &transport.AppendEntriesResponse{Term: 5, Success: false},
		})
		if err != nil {
			t.Fatal(err)
		}

		if n.currentTerm != 5 {
			t.Errorf("expected term 5 after step down, got %d", n.currentTerm)
		}
		if n.role != "follower" {
			t.Errorf("expected follower after higher term, got %q", n.role)
		}
		if n.votedFor != "" {
			t.Errorf("expected votedFor to be cleared, got %q", n.votedFor)
		}
	})
}

func TestTryAdvanceCommitIndex(t *testing.T) {

	t.Run("advances when majority matchIndex reaches current-term entry", func(t *testing.T) {
		n := newTestNodeWithEntries(t, 5)

		n.currentTerm = 3
		n.peers = []string{"p1", "p2", "p3"}
		n.nextIndex = map[string]int32{"p1": 5, "p2": 3, "p3": 4}
		n.matchIndex = map[string]int32{"p1": 4, "p2": 2, "p3": 3}

		err := n.tryAdvanceCommitIndex()
		if err != nil {
			t.Fatal(err)
		}

		if n.commitIndex != 3 {
			t.Errorf("expected commitIndex to be 3, got %d", n.commitIndex)
		}
	})

	t.Run("does not advance without majority", func(t *testing.T) {
		n := newTestNodeWithEntries(t, 4)

		n.currentTerm = 3
		n.peers = []string{"p1", "p2", "p3"}
		n.nextIndex = map[string]int32{"p1": 5, "p2": 2, "p3": 2}
		n.matchIndex = map[string]int32{"p1": 4, "p2": 2, "p3": 2}

		err := n.tryAdvanceCommitIndex()
		if err != nil {
			t.Fatal(err)
		}

		if n.commitIndex != 0 {
			t.Errorf("expected commitIndex to remain 0, got %d", n.commitIndex)
		}
	})

	t.Run("does not advance for an older-term candidate", func(t *testing.T) {
		n := newTestNodeWithEntries(t, 5)

		n.peers = []string{"p1", "p2", "p3"}
		n.nextIndex = map[string]int32{"p1": 5, "p2": 3, "p3": 4}
		n.matchIndex = map[string]int32{"p1": 4, "p2": 2, "p3": 3}

		err := n.tryAdvanceCommitIndex()
		if err != nil {
			t.Fatal(err)
		}

		if n.commitIndex != 0 {
			t.Errorf("expected commitIndex to remain at 0, got %d", n.commitIndex)
		}

	})
}

func TestApplyCommittedEntries(t *testing.T) {
	t.Run("applies entries between lastApplied and commitIndex", func(t *testing.T) {
		n := newTestNodeWithEntries(t, 3)
		n.commitIndex = 2

		if err := n.applyCommittedEntries(); err != nil {
			t.Fatal(err)
		}

		if n.lastApplied != 2 {
			t.Errorf("expected lastApplied to be 2, got %d", n.lastApplied)
		}
		if got, ok := n.store.Get("1"); !ok || got != "v" {
			t.Errorf("expected key 1 to be applied with value v, got value=%q ok=%t", got, ok)
		}
		if got, ok := n.store.Get("2"); !ok || got != "v" {
			t.Errorf("expected key 2 to be applied with value v, got value=%q ok=%t", got, ok)
		}
		if _, ok := n.store.Get("3"); ok {
			t.Error("expected key 3 to remain unapplied")
		}
	})

	t.Run("does not reapply entries that are already applied", func(t *testing.T) {
		n := newTestNodeWithEntries(t, 3)
		n.commitIndex = 3
		n.lastApplied = 2

		if err := n.applyCommittedEntries(); err != nil {
			t.Fatal(err)
		}

		if n.lastApplied != 3 {
			t.Errorf("expected lastApplied to be 3, got %d", n.lastApplied)
		}
		if _, ok := n.store.Get("1"); ok {
			t.Error("expected key 1 to remain unapplied because lastApplied was already 2")
		}
		if _, ok := n.store.Get("2"); ok {
			t.Error("expected key 2 to remain unapplied because lastApplied was already 2")
		}
		if got, ok := n.store.Get("3"); !ok || got != "v" {
			t.Errorf("expected key 3 to be applied with value v, got value=%q ok=%t", got, ok)
		}
	})
}
