package raft

import (
	"encoding/json"
	"errors"
	"os"
)

type Snapshot struct {
	// LastIncludedIndex is the highest Raft log index represented by this snapshot.
	LastIncludedIndex int32 `json:"lastIncludedIndex"`
	// LastIncludedTerm is the term for LastIncludedIndex, used in log consistency checks.
	LastIncludedTerm int32 `json:"lastIncludedTerm"`
	// State is the key-value state machine contents at LastIncludedIndex.
	State map[string]string `json:"state"`
}

// loadSnapshot reads the latest snapshot from disk, returning nil when none exists.
func (n *Node) loadSnapshot(path string) (*Snapshot, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	defer file.Close()

	var snapshot Snapshot
	if err := json.NewDecoder(file).Decode(&snapshot); err != nil {
		return nil, err
	}
	return &snapshot, nil
}

// restoreSnapshot installs a snapshot into the in-memory state machine.
func (n *Node) restoreSnapshot(snapshot *Snapshot) error {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	if snapshot == nil {
		return nil
	}

	n.store.ReplaceAll(snapshot.State)
	n.commitIndex = snapshot.LastIncludedIndex
	n.lastApplied = snapshot.LastIncludedIndex
	return nil
}

// restoreSnapshotFromDisk takes the current snapshot and applies it.
func (n *Node) restoreSnapshotFromDisk() error {
	snapshotPath, err := n.snapshotPath()
	if err != nil {
		return err
	}

	snapshot, err := n.loadSnapshot(snapshotPath)
	if err != nil {
		return err
	}

	return n.restoreSnapshot(snapshot)
}
