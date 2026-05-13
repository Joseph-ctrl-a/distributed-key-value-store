package raft

import "testing"

func TestLastLogIndex(t *testing.T) {
	t.Run("without snapshot uses wal length", func(t *testing.T) {
		n := newTestNodeWithEntries(t, 3)

		if got := n.lastLogIndex(); got != 3 {
			t.Errorf("expected 3, got %d", got)
		}
	})

	t.Run("with snapshot adds wal length to snapshot index", func(t *testing.T) {
		n := newTestNodeWithEntries(t, 2)
		n.lastSnapshotIndex = 5

		if got := n.lastLogIndex(); got != 7 {
			t.Errorf("expected 7, got %d", got)
		}
	})
}

func TestLastLogTerm(t *testing.T) {
	t.Run("without snapshot uses the wal to find last term", func(t *testing.T) {
		n := newTestNodeWithEntries(t, 3)

		got, err := n.lastLogTerm()
		if err != nil {
			t.Fatal(err)
		}
		if got != 3 {
			t.Errorf("expected 3, got %d", got)
		}
	})

	t.Run("with snapshot uses the wal to find last term", func(t *testing.T) {
		n := newTestNodeWithEntries(t, 2)
		n.lastSnapshotIndex = 5

		got, err := n.lastLogTerm()

		if err != nil {
			t.Fatal(err)
		}

		if got != 2 {
			t.Errorf("expected 2, got %d", got)
		}
	})
}

func TestTermAtIndex(t *testing.T) {
	t.Run("snapshot index returns snapshot term", func(t *testing.T) {
		n := newTestNodeWithEntries(t, 2)
		n.lastSnapshotIndex = 5
		n.lastSnapshotTerm = 3

		got, err := n.termAtIndex(5)
		if err != nil {
			t.Fatal(err)
		}
		if got != 3 {
			t.Errorf("expected snapshot term 3, got %d", got)
		}
	})

	t.Run("after snapshot maps raft index to wal offset", func(t *testing.T) {
		n := newTestNodeWithEntries(t, 2)
		n.lastSnapshotIndex = 5
		n.lastSnapshotTerm = 3

		got, err := n.termAtIndex(7)
		if err != nil {
			t.Fatal(err)
		}
		if got != 2 {
			t.Errorf("expected wal term 2, got %d", got)
		}
	})

	t.Run("before snapshot returns compacted error", func(t *testing.T) {
		n := newTestNodeWithEntries(t, 2)
		n.lastSnapshotIndex = 5
		n.lastSnapshotTerm = 3

		if _, err := n.termAtIndex(4); err == nil {
			t.Fatal("expected compacted index error")
		}
	})

	t.Run("missing wal offset returns error", func(t *testing.T) {
		n := newTestNodeWithEntries(t, 2)
		n.lastSnapshotIndex = 5
		n.lastSnapshotTerm = 3

		if _, err := n.termAtIndex(8); err == nil {
			t.Fatal("expected missing wal index error")
		}
	})
}

func TestEntriesFromIndex(t *testing.T) {
	t.Run("without snapshot returns entries from wal index", func(t *testing.T) {
		n := newTestNodeWithEntries(t, 3)

		entries, err := n.entriesFromIndex(2)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 2 {
			t.Fatalf("expected 2 entries, got %d", len(entries))
		}
		if entries[0] != "SET:2,v:2" {
			t.Errorf("expected first entry at raft index 2, got %q", entries[0])
		}
		if entries[1] != "SET:3,v:3" {
			t.Errorf("expected second entry at raft index 3, got %q", entries[1])
		}
	})

	t.Run("with snapshot maps raft index to wal offset", func(t *testing.T) {
		n := newTestNodeWithEntries(t, 3)
		n.lastSnapshotIndex = 5

		entries, err := n.entriesFromIndex(7)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 2 {
			t.Fatalf("expected 2 entries, got %d", len(entries))
		}
		if entries[0] != "SET:2,v:2" {
			t.Errorf("expected first entry at wal offset 2, got %q", entries[0])
		}
		if entries[1] != "SET:3,v:3" {
			t.Errorf("expected second entry at wal offset 3, got %q", entries[1])
		}
	})

	t.Run("snapshot index returns compacted error", func(t *testing.T) {
		n := newTestNodeWithEntries(t, 3)
		n.lastSnapshotIndex = 5

		if _, err := n.entriesFromIndex(5); err == nil {
			t.Fatal("expected compacted index error")
		}
	})

	t.Run("before snapshot returns compacted error", func(t *testing.T) {
		n := newTestNodeWithEntries(t, 3)
		n.lastSnapshotIndex = 5

		if _, err := n.entriesFromIndex(4); err == nil {
			t.Fatal("expected compacted index error")
		}
	})
}
