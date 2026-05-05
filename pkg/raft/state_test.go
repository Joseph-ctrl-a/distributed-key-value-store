package raft

import (
	"os"
	"testing"
)

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
