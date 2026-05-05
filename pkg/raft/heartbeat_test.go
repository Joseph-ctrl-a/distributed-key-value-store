package raft

import (
	"Distributed_Key_Value_Store/pkg/transport"
	"context"
	"testing"
)

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
