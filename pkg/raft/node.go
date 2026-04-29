package raft

import (
	"Distributed_Key_Value_Store/pkg/transport"
	"Distributed_Key_Value_Store/pkg/wal"

	"flag"
	"strings"
	"time"
)

type Node struct {
	transport.UnimplementedRaftServer
	id            string
	role          string
	currentLeader string
	currentTerm   int
	votedFor      string
	peers         []string
	log           *wal.Wal
	electionTimer time.Timer
	clients       map[string]transport.RaftClient
}

// NewNode defines how a Node should look
func NewNode(wal *wal.Wal) (*Node, error) {
	id := flag.String("id", "", "the node's ip:port")
	peers := flag.String("peers", "", "comma separated list of peer addresses")

	flag.Parse()

	node := &Node{id: *id, peers: strings.Split(*peers, ","), role: "follower", currentLeader: "", votedFor: "", log: wal, electionTimer: *time.NewTimer(time.Millisecond * 150)}

	err := node.init()
	if err != nil {
		return nil, err
	}
	return node, nil
}

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
	return nil
}
