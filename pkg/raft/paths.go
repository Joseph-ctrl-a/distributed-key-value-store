package raft

import (
	"os"
	"path/filepath"
	"strings"
)

// sanitizeNodeID converts a Raft node ID into a safe directory name.
func sanitizeNodeID(id string) string {
	return strings.NewReplacer(":", "_", "/", "_", "\\", "_").Replace(id)
}

// dataDir returns the per-node directory used for durable Raft files.
func (n *Node) dataDir() (string, error) {
	idSanitized := sanitizeNodeID(n.id)
	dataDir := filepath.Join("data", idSanitized)

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return "", err
	}
	return dataDir, nil
}

// persistentStatePath returns the path for currentTerm/votedFor persistence.
func (n *Node) persistentStatePath() (string, error) {
	dataDir, err := n.dataDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dataDir, "state.json"), nil
}

// snapshotPath returns the path for the node's latest state-machine snapshot.
func (n *Node) snapshotPath() (string, error) {
	dataDir, err := n.dataDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dataDir, "snapshot.json"), nil
}
