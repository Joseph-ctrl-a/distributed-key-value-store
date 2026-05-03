package raft

import (
	"Distributed_Key_Value_Store/pkg/store"
	"Distributed_Key_Value_Store/pkg/transport"
	"Distributed_Key_Value_Store/pkg/wal"
	"flag"
	"strings"
	"sync"
	"time"
)

type Node struct {
	transport.UnimplementedRaftServer
	id              string
	role            string
	currentLeader   string
	currentTerm     int32
	votedFor        string
	peers           []string
	log             *wal.Wal
	electionTimer   *time.Timer
	clients         map[string]transport.RaftClient
	persistentState *PersistentState
	mutex           sync.RWMutex
	commitIndex     int32
	store           *store.HashMap
	lastApplied     int32
	nextIndex       map[string]int32
	matchIndex      map[string]int32
}

// NewNode defines how a Node should look
func NewNode(wal *wal.Wal) (*Node, error) {

	// Get CL params
	id := flag.String("id", "", "the node's ip:port")
	peers := flag.String("peers", "", "comma separated list of peer addresses")
	flag.Parse()

	node := &Node{id: *id, peers: strings.Split(*peers, ","), role: "follower", currentLeader: "", votedFor: "", log: wal, commitIndex: 0}

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
	n.startElectionTimer()

	state, err := NewPersistentState()

	if err != nil {
		return err
	}
	n.persistentState = state

	n.electionTimer = time.NewTimer(time.Millisecond * time.Duration(n.RandomTime(150, 150)))

	n.store = store.NewHashMap()
	return nil

}
