package raft

import (
	"os"
	"path/filepath"
	"strings"
)

func sanitizeNodeId(id string) string {
	return strings.NewReplacer(":", "_", "/", "_", "\\", "_").Replace(id)
}
func (n *Node) dataDir() (string, error) {
	idSanitized := sanitizeNodeId(n.id)
	dataDir := filepath.Join("data", idSanitized)

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return "", err
	}
	return dataDir, nil
}

func (n *Node) persistentStatePath() (string, error) {
	dataDir, err := n.dataDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dataDir, "state.json"), nil
}

func (n *Node) snapshotPath() (string, error) {
	dataDir, err := n.dataDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dataDir, "snapshot.json"), nil
}
