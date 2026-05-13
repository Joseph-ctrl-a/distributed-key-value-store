package raft

import (
	"Distributed_Key_Value_Store/pkg/store"
	"Distributed_Key_Value_Store/pkg/transport"
	"Distributed_Key_Value_Store/pkg/wal"
	"sync"
	"time"
)

type Node struct {
	transport.UnimplementedRaftServer
	id                string
	role              string
	currentLeader     string
	currentTerm       int32
	votedFor          string
	peers             []string
	log               *wal.Wal
	electionTimer     *time.Timer
	clients           map[string]transport.RaftClient
	persistentState   *PersistentState
	mutex             sync.RWMutex
	commitIndex       int32
	store             *store.HashMap
	lastApplied       int32
	nextIndex         map[string]int32
	matchIndex        map[string]int32
	blockedPeers      map[string]bool
	lastSnapshotIndex int32
	lastSnapshotTerm  int32
}

// NewNode defines how a Node should look
func NewNode(id string, peers []string, wal *wal.Wal) (*Node, error) {

	node := &Node{id: id, peers: peers, role: "follower", currentLeader: "", votedFor: "", log: wal, commitIndex: 0, blockedPeers: make(map[string]bool)}

	err := node.init()
	if err != nil {
		return nil, err
	}
	return node, nil
}

// init sets up peer connections, starts the gRPC server, election timer, and loads persistent state.
func (n *Node) init() error {
	err := n.createConnections()
	if err != nil {
		return err
	}

	err = n.startServer()
	if err != nil {
		return err
	}
	statePath, err := n.persistentStatePath()

	if err != nil {
		return err
	}

	state, err := NewPersistentState(statePath)

	if err != nil {
		return err
	}
	n.persistentState = state
	n.currentTerm = state.CurrentTerm
	n.votedFor = state.VotedFor

	n.electionTimer = time.NewTimer(time.Millisecond * time.Duration(n.RandomTime(1500, 1500)))

	n.startElectionTimer()

	n.store = store.NewHashMap()
	if err := n.restoreSnapshotFromDisk(); err != nil {
		return err
	}

	return nil

}
