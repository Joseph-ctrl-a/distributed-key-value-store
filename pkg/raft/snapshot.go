package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

const snapshotThreshold int32 = 5

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
		return nil, fmt.Errorf("open snapshot file: %w", err)
	}

	defer file.Close()

	var snapshot Snapshot
	if err := json.NewDecoder(file).Decode(&snapshot); err != nil {
		return nil, fmt.Errorf("decode snapshot file: %w", err)
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
	n.lastSnapshotIndex = snapshot.LastIncludedIndex
	n.lastSnapshotTerm = snapshot.LastIncludedTerm
	return nil
}

// restoreSnapshotFromDisk takes the current snapshot and applies it.
func (n *Node) restoreSnapshotFromDisk() error {
	snapshotPath, err := n.snapshotPath()
	if err != nil {
		return fmt.Errorf("snapshot path: %w", err)
	}

	snapshot, err := n.loadSnapshot(snapshotPath)
	if err != nil {
		return fmt.Errorf("load snapshot: %w", err)
	}

	if err := n.restoreSnapshot(snapshot); err != nil {
		return fmt.Errorf("restore snapshot: %w", err)
	}
	return nil
}

// saveSnapshot saves a snapshot to a given path
func (n *Node) saveSnapshot(snapshot Snapshot, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create snapshot file: %w", err)
	}
	defer file.Close()

	if err := json.NewEncoder(file).Encode(snapshot); err != nil {
		return fmt.Errorf("encode snapshot: %w", err)
	}
	return nil
}

// newSnapshot creates a new snapshot based off the current node state
func (n *Node) newSnapshot(index int32) (*Snapshot, error) {
	term, err := n.log.GetTermAtIndex(index)

	if err != nil {
		return nil, fmt.Errorf("get snapshot term at index %d: %w", index, err)
	}

	snapshot := &Snapshot{
		LastIncludedIndex: index,
		LastIncludedTerm:  term,
		State:             n.store.All(),
	}
	return snapshot, nil
}

// shouldSnapshot checks whether its viable to snapshot
func (n *Node) shouldSnapshot() bool {
	n.mutex.RLock()
	defer n.mutex.RUnlock()
	return n.lastApplied-n.lastSnapshotIndex >= snapshotThreshold
}

// maybeSnapshot writes a snapshot when enough new entries have been applied.
func (n *Node) maybeSnapshot() error {
	n.mutex.RLock()
	shouldSnapshot := n.lastApplied-n.lastSnapshotIndex >= snapshotThreshold
	lastApplied := n.lastApplied
	n.mutex.RUnlock()

	if !shouldSnapshot {
		return nil
	}

	snapshot, err := n.newSnapshot(lastApplied)

	if err != nil {
		return fmt.Errorf("create snapshot: %w", err)
	}

	path, err := n.snapshotPath()

	if err != nil {
		return fmt.Errorf("snapshot path: %w", err)
	}

	err = n.saveSnapshot(*snapshot, path)

	if err != nil {
		return fmt.Errorf("save snapshot: %w", err)
	}
	n.mutex.Lock()
	n.lastSnapshotIndex = snapshot.LastIncludedIndex
	n.lastSnapshotTerm = snapshot.LastIncludedTerm
	n.mutex.Unlock()
	return nil
}
