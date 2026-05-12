package raft

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSanitizeNodeID(t *testing.T) {
	tests := []struct {
		name string
		id   string
		want string
	}{
		{"host port", "localhost:5001", "localhost_5001"},
		{"forward slash", "node/a", "node_a"},
		{"backslash", `node\a`, "node_a"},
		{"mixed separators", `host:5001/peer\one`, "host_5001_peer_one"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sanitizeNodeID(tt.id); got != tt.want {
				t.Errorf("expected %q, got %q", tt.want, got)
			}
		})
	}
}

func TestNodeDataDir(t *testing.T) {
	t.Chdir(t.TempDir())

	n := &Node{id: "localhost:5001"}
	got, err := n.dataDir()
	if err != nil {
		t.Fatal(err)
	}

	want := filepath.Join("data", "localhost_5001")
	if got != want {
		t.Errorf("expected %q, got %q", want, got)
	}

	info, err := os.Stat(got)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() {
		t.Fatalf("expected %q to be a directory", got)
	}
}

func TestNodePersistentStatePath(t *testing.T) {
	t.Chdir(t.TempDir())

	n := &Node{id: "localhost:5001"}
	got, err := n.persistentStatePath()
	if err != nil {
		t.Fatal(err)
	}

	want := filepath.Join("data", "localhost_5001", "state.json")
	if got != want {
		t.Errorf("expected %q, got %q", want, got)
	}
}

func TestNodeSnapshotPath(t *testing.T) {
	t.Chdir(t.TempDir())

	n := &Node{id: "localhost:5001"}
	got, err := n.snapshotPath()
	if err != nil {
		t.Fatal(err)
	}

	want := filepath.Join("data", "localhost_5001", "snapshot.json")
	if got != want {
		t.Errorf("expected %q, got %q", want, got)
	}
}
