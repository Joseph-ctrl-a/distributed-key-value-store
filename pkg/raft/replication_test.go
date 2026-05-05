package raft

import (
	"Distributed_Key_Value_Store/pkg/transport"
	"testing"
)

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
