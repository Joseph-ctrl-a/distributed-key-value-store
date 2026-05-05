package raft

import (
	"Distributed_Key_Value_Store/pkg/store"
	"Distributed_Key_Value_Store/pkg/transport"
	"Distributed_Key_Value_Store/pkg/wal"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func newTestNode(t *testing.T) *Node {
	t.Helper()

	w, err := wal.NewWal(filepath.Join(t.TempDir(), "wal.log"))
	if err != nil {
		t.Fatal(err)
	}

	stateFile, err := os.CreateTemp(t.TempDir(), "state*.json")
	if err != nil {
		t.Fatal(err)
	}

	ps := &PersistentState{file: stateFile}

	n := &Node{
		id:              "node-1",
		role:            "follower",
		currentTerm:     0,
		votedFor:        "",
		log:             w,
		persistentState: ps,
		store:           store.NewHashMap(),
		electionTimer:   time.NewTimer(24 * time.Hour),
	}

	t.Cleanup(func() {
		w.Close()
		stateFile.Close()
	})

	return n
}

func newTestNodeWithEntries(t *testing.T, count int) *Node {
	t.Helper()
	n := newTestNode(t)
	for i := 1; i <= count; i++ {
		if err := n.log.Append(wal.NewLogEntry("SET", []string{strconv.Itoa(i), "v"}, i)); err != nil {
			t.Fatal(err)
		}
	}
	n.currentTerm = int32(count)
	return n
}

func voteReq(term int32, candidateId string, lastLogIndex, lastLogTerm int32) *transport.RequestVoteRequest {
	return &transport.RequestVoteRequest{
		Term:         term,
		CandidateId:  candidateId,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
}
