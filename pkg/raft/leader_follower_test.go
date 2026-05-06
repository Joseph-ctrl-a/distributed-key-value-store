package raft

import (
	"Distributed_Key_Value_Store/pkg/transport"
	"testing"
)

func TestLeaderReplicatesCommittedEntryToFollower(t *testing.T) {
	leader := newTestNodeWithEntries(t, 1)
	follower := newTestNode(t)

	leader.id = "leader"
	leader.role = "leader"
	leader.currentTerm = 1
	leader.commitIndex = 1
	leader.peers = []string{"follower"}
	leader.nextIndex = map[string]int32{"follower": 1}
	leader.matchIndex = map[string]int32{"follower": 0}

	req, err := leader.newAppendEntriesRequest("follower")
	if err != nil {
		t.Fatal(err)
	}

	res, err := follower.handleEntry(req)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Success {
		t.Fatalf("expected follower to accept append entries, got success=false term=%d", res.Term)
	}

	if got := follower.log.LastLogIndex(); got != 1 {
		t.Errorf("expected follower log index 1, got %d", got)
	}
	if follower.commitIndex != 1 {
		t.Errorf("expected follower commitIndex 1, got %d", follower.commitIndex)
	}
	if follower.lastApplied != 1 {
		t.Errorf("expected follower lastApplied 1, got %d", follower.lastApplied)
	}
	if got, ok := follower.store.Get("1"); !ok || got != "v" {
		t.Errorf("expected replicated key 1 to be applied with value v, got value=%q ok=%t", got, ok)
	}
}

func TestLeaderCatchesUpFollowerFromPartialLog(t *testing.T) {
	leader := newTestNodeWithEntries(t, 3)
	follower := newTestNodeWithEntries(t, 2)

	leader.id = "leader"
	leader.role = "leader"
	leader.currentTerm = 3
	leader.commitIndex = 3
	leader.peers = []string{"follower"}
	leader.nextIndex = map[string]int32{"follower": 3}
	leader.matchIndex = map[string]int32{"follower": 2}

	req, err := leader.newAppendEntriesRequest("follower")
	if err != nil {
		t.Fatal(err)
	}
	if len(req.Entries) != 1 {
		t.Fatalf("expected one missing entry, got %d", len(req.Entries))
	}

	res, err := follower.handleEntry(req)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Success {
		t.Fatalf("expected follower to accept catch-up append, got success=false term=%d", res.Term)
	}

	if got := follower.log.LastLogIndex(); got != 3 {
		t.Errorf("expected follower log index 3, got %d", got)
	}
	if follower.commitIndex != 3 {
		t.Errorf("expected follower commitIndex 3, got %d", follower.commitIndex)
	}
	if got, ok := follower.store.Get("3"); !ok || got != "v" {
		t.Errorf("expected key 3 to be applied with value v, got value=%q ok=%t", got, ok)
	}
}

func TestFollowerRejectsMismatchedPreviousLog(t *testing.T) {
	follower := newTestNodeWithEntries(t, 2)

	res, err := follower.handleEntry(&transport.AppendEntriesRequest{
		Term:         2,
		LeaderId:     "leader",
		Entries:      []string{"SET:3,v:2"},
		PrevLogIndex: 2,
		PrevLogTerm:  99,
		LeaderCommit: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Success {
		t.Fatal("expected follower to reject append with mismatched previous log term")
	}
	if got := follower.log.LastLogIndex(); got != 2 {
		t.Errorf("expected follower log to remain at index 2, got %d", got)
	}
}

func TestLeaderBacktracksAndRetriesFollowerReplication(t *testing.T) {
	leader := newTestNodeWithEntries(t, 3)
	follower := newTestNodeWithEntries(t, 1)

	leader.id = "leader"
	leader.role = "leader"
	leader.currentTerm = 3
	leader.commitIndex = 3
	leader.peers = []string{"follower"}
	leader.nextIndex = map[string]int32{"follower": 3}
	leader.matchIndex = map[string]int32{"follower": 1}

	req, err := leader.newAppendEntriesRequest("follower")
	if err != nil {
		t.Fatal(err)
	}
	res, err := follower.handleEntry(req)
	if err != nil {
		t.Fatal(err)
	}
	if res.Success {
		t.Fatal("expected first append to fail because follower is missing prevLogIndex")
	}

	if err := leader.handleAppendEntriesResult(&appendEntryResult{
		peer:     "follower",
		request:  req,
		response: res,
	}); err != nil {
		t.Fatal(err)
	}
	if got := leader.nextIndex["follower"]; got != 2 {
		t.Fatalf("expected nextIndex to backtrack to 2, got %d", got)
	}

	req, err = leader.newAppendEntriesRequest("follower")
	if err != nil {
		t.Fatal(err)
	}
	res, err = follower.handleEntry(req)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Success {
		t.Fatalf("expected retry append to succeed, got success=false term=%d", res.Term)
	}

	if err := leader.handleAppendEntriesResult(&appendEntryResult{
		peer:     "follower",
		request:  req,
		response: res,
	}); err != nil {
		t.Fatal(err)
	}
	if got := follower.log.LastLogIndex(); got != 3 {
		t.Errorf("expected follower log index 3 after retry, got %d", got)
	}
	if got := leader.matchIndex["follower"]; got != 3 {
		t.Errorf("expected leader matchIndex 3 after retry, got %d", got)
	}
	if got := leader.nextIndex["follower"]; got != 4 {
		t.Errorf("expected leader nextIndex 4 after retry, got %d", got)
	}
}
