package raft

import (
	"Distributed_Key_Value_Store/pkg/store"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadSnapshot(t *testing.T) {
	t.Run("missing file returns nil snapshot", func(t *testing.T) {
		n := newTestNode(t)

		snapshot, err := n.loadSnapshot(filepath.Join(t.TempDir(), "missing.json"))
		if err != nil {
			t.Fatal(err)
		}
		if snapshot != nil {
			t.Fatalf("expected nil snapshot, got %#v", snapshot)
		}
	})

	t.Run("loads snapshot from json", func(t *testing.T) {
		n := newTestNode(t)
		path := filepath.Join(t.TempDir(), "snapshot.json")

		want := Snapshot{
			LastIncludedIndex: 7,
			LastIncludedTerm:  3,
			State: map[string]string{
				"x": "10",
				"y": "20",
			},
		}

		file, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.NewEncoder(file).Encode(want); err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}

		got, err := n.loadSnapshot(path)
		if err != nil {
			t.Fatal(err)
		}
		if got.LastIncludedIndex != want.LastIncludedIndex {
			t.Errorf("expected index %d, got %d", want.LastIncludedIndex, got.LastIncludedIndex)
		}
		if got.LastIncludedTerm != want.LastIncludedTerm {
			t.Errorf("expected term %d, got %d", want.LastIncludedTerm, got.LastIncludedTerm)
		}
		if got.State["x"] != "10" || got.State["y"] != "20" {
			t.Errorf("unexpected state: %#v", got.State)
		}
	})

	t.Run("invalid json returns error", func(t *testing.T) {
		n := newTestNode(t)
		path := filepath.Join(t.TempDir(), "snapshot.json")

		if err := os.WriteFile(path, []byte("{bad json"), 0644); err != nil {
			t.Fatal(err)
		}

		if _, err := n.loadSnapshot(path); err == nil {
			t.Fatal("expected invalid json error")
		}
	})
}

func TestRestoreSnapshot(t *testing.T) {
	t.Run("nil snapshot is no-op", func(t *testing.T) {
		n := newTestNode(t)
		n.commitIndex = 2
		n.lastApplied = 2
		n.store.Set("existing", "value")

		if err := n.restoreSnapshot(nil); err != nil {
			t.Fatal(err)
		}

		if n.commitIndex != 2 || n.lastApplied != 2 {
			t.Errorf("expected indexes unchanged, got commit=%d applied=%d", n.commitIndex, n.lastApplied)
		}
	})

	t.Run("restores store and raft indexes", func(t *testing.T) {
		n := newTestNode(t)
		n.store.Set("old", "value")

		snapshot := &Snapshot{
			LastIncludedIndex: 5,
			LastIncludedTerm:  2,
			State: map[string]string{
				"x": "10",
			},
		}

		if err := n.restoreSnapshot(snapshot); err != nil {
			t.Fatal(err)
		}

		if n.commitIndex != 5 {
			t.Errorf("expected commitIndex 5, got %d", n.commitIndex)
		}
		if n.lastApplied != 5 {
			t.Errorf("expected lastApplied 5, got %d", n.lastApplied)
		}
		if n.store.Has("old") {
			t.Error("expected old key to be removed")
		}
		got, ok := n.store.Get("x")
		if !ok || got != "10" {
			t.Errorf("expected x=10, got %q ok=%t", got, ok)
		}
	})
}

func TestRestoreSnapshotFromDisk(t *testing.T) {
	t.Chdir(t.TempDir())

	n := newTestNode(t)
	n.id = "localhost:5001"
	n.store = store.NewHashMap()

	path, err := n.snapshotPath()
	if err != nil {
		t.Fatal(err)
	}

	snapshot := Snapshot{
		LastIncludedIndex: 4,
		LastIncludedTerm:  2,
		State: map[string]string{
			"a": "1",
		},
	}

	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewEncoder(file).Encode(snapshot); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	if err := n.restoreSnapshotFromDisk(); err != nil {
		t.Fatal(err)
	}

	if n.commitIndex != 4 {
		t.Errorf("expected commitIndex 4, got %d", n.commitIndex)
	}
	if n.lastApplied != 4 {
		t.Errorf("expected lastApplied 4, got %d", n.lastApplied)
	}

	got, ok := n.store.Get("a")
	if !ok || got != "1" {
		t.Errorf("expected a=1, got %q ok=%t", got, ok)
	}
}

func TestSaveSnapshot(t *testing.T) {
	t.Chdir(t.TempDir())
	n := newTestNode(t)
	n.id = "localhost:5001"
	n.store = store.NewHashMap()

	path, err := n.snapshotPath()
	if err != nil {
		t.Fatal(err)
	}

	want := Snapshot{
		LastIncludedIndex: 4,
		LastIncludedTerm:  2,
		State: map[string]string{
			"a": "1",
		},
	}

	err = n.saveSnapshot(want, path)

	if err != nil {
		t.Fatal(err)
	}

	file, err := os.Open(path)

	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var got Snapshot
	if err := json.NewDecoder(file).Decode(&got); err != nil {
		t.Fatal(err)
	}

	if got.LastIncludedIndex != want.LastIncludedIndex {
		t.Errorf("expected index %d, got %d", want.LastIncludedIndex, got.LastIncludedIndex)
	}
	if got.LastIncludedTerm != want.LastIncludedTerm {
		t.Errorf("expected term %d, got %d", want.LastIncludedTerm, got.LastIncludedTerm)
	}
	if got.State["a"] != "1" {
		t.Errorf("expected a=1, got state %#v", got.State)
	}
}
